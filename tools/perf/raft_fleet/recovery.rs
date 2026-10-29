// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit owned recovery startup; evidence and physical archive replay remain mandatory.
use super::*;
use cellar::sdk::consensus::verification::ArchiveVerifier;
use cellar_ltx::generation::{GenerationLimits, GenerationStore, ImageId};
use cellar_ltx::{DirectFileSystem, LtxHost};

impl Api {
    pub(super) fn recovery_status(&self) -> Result<Value> {
        let guard = self
            .recovery
            .lock()
            .map_err(|_| anyhow::anyhow!("recovery handle poisoned"))?;
        let Some(service) = guard.as_ref() else {
            return Ok(Value::Null);
        };
        let status = service.status();
        Ok(
            json!({"running":status.running, "rounds":status.poll_rounds,
            "execution_attempts":status.execution_attempts, "settled_movements":status.settled_movements,
            "last_revision":status.last_revision,
            "last_failure":status.last_round_failure.map(|f| format!("{f:?}")),
            "detection":status.failure_detection.map(|d| json!({
                "revision":d.observed_revision,
                "nodes":d.nodes.iter().map(|n| json!({
                    "boot":n.boot, "health":format!("{:?}",n.health),
                    "consecutive_failures":n.consecutive_failures,
                    "last_error":n.last_error.as_ref().map(|e| format!("{e:?}")),
                    "sample_age_ms":n.sample_age_ms
                })).collect::<Vec<_>>()
            })),
            "recovery":status.failure_recovery.map(|r| json!({
                "phase":format!("{:?}",r.phase), "verified_histories":r.verified_histories,
                "total_histories":r.total_histories, "removed_revision":r.removed_revision
            }))}),
        )
    }

    pub(super) async fn start_recovery(&self) -> Result<Value> {
        if self
            .recovery
            .lock()
            .map_err(|_| anyhow::anyhow!("recovery handle poisoned"))?
            .is_some()
        {
            return self.recovery_status();
        }
        let cut = self.managed.catalog.read(TIMEOUT).await?;
        let host = self
            .managed
            .catalog
            .authorize_host(&cut, self.node, TIMEOUT)
            .await?;
        ensure!(
            host.state() == FleetNodeState::Active,
            "recovery requires active local capacity"
        );
        self.resources.observe(self.node, TIMEOUT).await?;
        self.managed
            .catalog
            .check_inventory(&cut, std::slice::from_ref(&self.managed.namespace), TIMEOUT)
            .await?;
        // Replay storage is deployment-owned and retained across controller
        // recreation. Its configured ceilings must be included in the supplied
        // durable overhead policy; opening it grants no archive coverage.
        let image_bytes = self.managed.config.image_bytes;
        let replay_extent_bytes = self.managed.config.replay_extent_bytes;
        let verifier = self
            .managed
            .domain
            .blocking(move || -> Result<_> {
                let mut limits = GenerationLimits {
                    max_bytes: image_bytes * 8,
                    ..Default::default()
                };
                limits.apply.decode.max_image_bytes = image_bytes;
                let scratch = GenerationStore::open(
                    PathBuf::from("/data/fleet-replay/images"),
                    Arc::new(DirectFileSystem),
                    limits,
                )?;
                let extents = cellar_ltx::internal::NodeExtentStore::open(
                    PathBuf::from("/data/fleet-replay/extents"),
                    Arc::new(DirectFileSystem),
                    RaftExtentLimits {
                        max_bytes: replay_extent_bytes,
                        ..Default::default()
                    },
                )?;
                Ok(ArchiveVerifier::new(scratch, ImageId::new([91; 16])?)?
                    .with_extent_store(extents)?)
            })?
            .await??;
        let resources = Arc::new(self.resources.clone());
        let service = self
            .managed
            .catalog
            .start_failure_recovery_service(
                self.cells.clone(),
                resources.clone(),
                resources,
                Arc::new(self.isolation.clone()),
                FleetMovementServiceOptions::default(),
                FleetFailureRecoveryOptions {
                    detection: FleetFailureDetectionOptions::default(),
                    archive_host: LtxHost::default(),
                    archive_verifier: verifier,
                    archive_limits: ArchiveLimits {
                        max_image_bytes: image_bytes,
                        ..Default::default()
                    },
                },
            )
            .await?;
        *self
            .recovery
            .lock()
            .map_err(|_| anyhow::anyhow!("recovery handle poisoned"))? = Some(service);
        self.recovery_status()
    }
}
