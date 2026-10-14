//! Host-owned stop receipts. These observations never grant cell authority.
use crate::bucket::Bucket;
use crate::management::{StopMode, StopProof, StopReceipt};
use crate::node_lifecycle::NodeOutcome;
use anyhow::{ensure, Context, Result};
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

pub(crate) struct NodeOperations {
    bucket: Bucket,
    node: String,
    generation: String,
    state: Mutex<Option<StopReceipt>>,
    progress: watch::Sender<Option<StopReceipt>>,
}

impl NodeOperations {
    #[cfg(not(cellar_internal_tests))]
    pub(crate) fn install(bucket: Bucket, node: String, generation: String) -> Result<Arc<Self>> {
        let (progress, mut updates) = watch::channel(None);
        let operations = Arc::new(Self {
            bucket,
            node,
            generation,
            state: Mutex::new(None),
            progress,
        });
        crate::asyncrt::services().configure_node_operations(operations.clone())?;
        let reporter = operations.clone();
        crate::asyncrt::try_spawn(async move {
            while updates.changed().await.is_ok() {
                let receipt = updates.borrow_and_update().clone();
                if let Some(receipt) = receipt {
                    if let Err(error) = reporter.publish_bounded(&receipt).await {
                        tracing::warn!(%error, "node stop progress was not confirmed");
                    }
                }
            }
        })?
        .detach();
        Ok(operations)
    }

    /// Reserve once before the first storage await. Cancellation leaves an
    /// uncertain request reserved; it cannot authorize a second stop operation.
    pub(crate) async fn begin(&self, operation_id: &str, mode: StopMode) -> Result<StopReceipt> {
        let key = crate::management::receipt_key(&self.node, &self.generation, operation_id)?;
        let now = crate::ownership_store::now_ms();
        let receipt = StopReceipt {
            version: 1,
            node: self.node.clone(),
            generation: self.generation.clone(),
            operation_id: operation_id.into(),
            requested_mode: mode,
            phase: "accepted".into(),
            sequence: 0,
            requested_at: now,
            observed_at: now,
            domain_joined: false,
            proof: None,
            error: None,
        };
        {
            let mut state = self
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("node operation state unavailable"))?;
            ensure!(
                state.is_none(),
                "a stop request is already reserved for this node generation; inspect its receipt"
            );
            *state = Some(receipt.clone());
        }
        crate::asyncrt::timeout(std::time::Duration::from_secs(5), async {
            self.bucket
                .put_cas(&key, serde_json::to_vec(&receipt)?, None)
                .await?
                .context("stop receipt already exists; inspect it before retrying")?;
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("stop receipt write timed out; outcome unknown")??;
        Ok(receipt)
    }

    /// Driver phases publish asynchronously and never hold a lock across I/O.
    pub(crate) fn phase(&self, phase: &str) {
        if let Ok(mut state) = self.state.lock() {
            if let Some(receipt) = state.as_mut() {
                receipt.sequence += 1;
                receipt.phase = phase.into();
                receipt.observed_at = crate::ownership_store::now_ms();
                self.progress.send_replace(Some(receipt.clone()));
            }
        }
    }

    pub(crate) fn drained(&self, outcome: &Result<NodeOutcome>) {
        if let Ok(mut state) = self.state.lock() {
            if let Some(receipt) = state.as_mut() {
                receipt.sequence += 1;
                receipt.phase = "joining_domain".into();
                receipt.observed_at = crate::ownership_store::now_ms();
                match outcome {
                    Ok(NodeOutcome::Stopped(report)) => {
                        receipt.proof = Some(StopProof::from(report))
                    }
                    other => receipt.error = Some(outcome_error(other)),
                }
                self.progress.send_replace(Some(receipt.clone()));
            }
        }
    }

    async fn publish_bounded(&self, receipt: &StopReceipt) -> Result<()> {
        crate::asyncrt::timeout(std::time::Duration::from_secs(5), self.publish(receipt))
            .await
            .context("stop receipt publication timed out")?
    }

    async fn publish(&self, receipt: &StopReceipt) -> Result<()> {
        let key =
            crate::management::receipt_key(&self.node, &self.generation, &receipt.operation_id)?;
        for _ in 0..3 {
            let (bytes, token) = self
                .bucket
                .get(&key)
                .await?
                .context("stop receipt disappeared")?;
            ensure!(bytes.len() <= 64 * 1024, "stop receipt exceeds 64 KiB");
            let current: StopReceipt = serde_json::from_slice(&bytes)?;
            ensure!(
                current.version == receipt.version
                    && current.node == receipt.node
                    && current.generation == receipt.generation
                    && current.operation_id == receipt.operation_id
                    && current.requested_mode == receipt.requested_mode,
                "stop receipt identity changed"
            );
            if current.sequence >= receipt.sequence {
                return Ok(());
            }
            if self
                .bucket
                .put_cas(&key, serde_json::to_vec(receipt)?, Some(&token))
                .await?
                .is_some()
            {
                return Ok(());
            }
        }
        anyhow::bail!("stop receipt changed concurrently")
    }

    /// Called by the surrounding host only after its node domain has joined.
    /// The separate reporting domain owns this last store write and its join.
    #[cfg(not(cellar_internal_tests))]
    pub(crate) async fn joined(
        &self,
        outcome: &Result<NodeOutcome>,
        executor: tokio::runtime::Handle,
    ) {
        let receipt = self.state.lock().ok().and_then(|state| state.clone());
        let Some(mut receipt) = receipt else {
            return;
        };
        receipt.domain_joined = true;
        receipt.sequence = u64::MAX;
        match outcome {
            Ok(NodeOutcome::Stopped(report)) => {
                let proof = StopProof::from(report);
                receipt.phase = if proof.complete(receipt.requested_mode) {
                    "complete"
                } else {
                    "incomplete"
                }
                .into();
                receipt.proof = Some(proof);
            }
            other => {
                receipt.phase = "failed".into();
                receipt.proof = None;
                receipt.error = Some(outcome_error(other));
            }
        }
        let reporting = crate::asyncrt::ExecutionDomain::new(executor);
        let result = reporting
            .run(async {
                receipt.observed_at = crate::ownership_store::now_ms();
                self.publish_bounded(&receipt).await
            })
            .await;
        if !matches!(result, Ok(Ok(()))) {
            tracing::warn!(?result, "joined node stop receipt was not confirmed");
        }
        reporting.shutdown().await;
    }
}

pub(super) fn phase(phase: &str) {
    if let Some(operations) = crate::asyncrt::services().node_operations() {
        operations.phase(phase);
    }
}

fn outcome_error(outcome: &Result<NodeOutcome>) -> String {
    match outcome {
        Ok(NodeOutcome::Fenced { code, reason }) => format!("node fenced ({code}): {reason:?}"),
        Ok(NodeOutcome::Cancelled) => "node domain was forcibly closed".into(),
        Ok(NodeOutcome::CoreFailed { .. }) => {
            "node core supervision failed; inspect host logs".into()
        }
        Ok(NodeOutcome::ReplicationFailed { .. }) => {
            "node replication failed; inspect host logs".into()
        }
        _ => "node supervisor failed; inspect host logs".into(),
    }
}
