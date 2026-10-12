// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Modified for Cellar.

use super::{ActorIdentity, ActorServices, AdmissionLimits, Ownership};
use crate::machine::{
    ownership_on_evict_from_environment, pressure_config_from_environment,
    random_process_generation, DEFAULT_MAX_OUTBOUND_WEBSOCKETS,
    DEFAULT_OWNER_LOG_RECOVERY_ATTEMPTS, DEFAULT_OWNER_LOG_RECOVERY_BACKOFF_MS,
};
use cellar_logic::{Config, OwnershipOnEvict};
use std::sync::Arc;

/// All lifecycle and lease policy for one embedded actor.
///
/// Install this policy through `domain.services().configure_actor(&options)`
/// before building application generations. The decision configuration is
/// supplied intact, including admission, eviction, pressure, alarm, recovery
/// and WebSocket limits. Generation identifies this lease-writing process;
/// a node-log ownership adapter requires its signing identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActorOptions {
    pub core: Config,
    pub lease_ttl_ms: u64,
    pub placement_weight: u64,
    pub generation: String,
    pub resume_generation: Option<String>,
}

impl ActorOptions {
    pub(super) async fn validate(
        &self,
        identity: &ActorIdentity,
        services: &ActorServices,
    ) -> anyhow::Result<()> {
        self.validate_policy()?;
        crate::machine::validate_node_name(&identity.node)?;
        crate::startup::parse_advertise(&identity.advertise)?;
        anyhow::ensure!(
            services.peer_auth.source() == identity.node,
            "actor and peer authentication must identify the same node"
        );
        if let Some(runtime) = &services.runtime {
            anyhow::ensure!(
                Arc::ptr_eq(runtime.host_services(), &crate::asyncrt::services()),
                "actor runtime belongs to another execution domain"
            );
            anyhow::ensure!(
                runtime.node.as_ref() == identity.node
                    && runtime.region.as_ref() == identity.region,
                "actor and runtime must identify the same node and region"
            );
        }
        match services.ownership.as_ref() {
            Some(Ownership::Bucket(bucket)) => {
                anyhow::ensure!(
                    bucket.node() == identity.node,
                    "actor and ownership adapter must identify the same node"
                );
                anyhow::ensure!(
                    bucket.lease_ttl_ms() == self.lease_ttl_ms,
                    "actor and ownership adapter must use the same lease TTL"
                );
                if let Some(generation) = bucket.process_generation() {
                    anyhow::ensure!(
                        generation == self.generation,
                        "actor lease generation must match the ownership signing identity"
                    );
                }
            }
            Some(Ownership::Memory(memory)) => {
                anyhow::ensure!(
                    memory.lock().await.node == identity.node,
                    "actor and ownership adapter must identify the same node"
                );
                self.require_local_ownership()?;
            }
            None => self.require_local_ownership()?,
        }
        Ok(())
    }

    fn require_local_ownership(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.core.ownership_on_evict == OwnershipOnEvict::Sticky,
            "local-only ownership must retain its sole copy of cell data"
        );
        Ok(())
    }

    pub(crate) fn validate_policy(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.lease_ttl_ms > 0, "node lease TTL must be positive");
        anyhow::ensure!(
            self.placement_weight > 0,
            "placement weight must be positive"
        );
        anyhow::ensure!(
            !self.generation.is_empty(),
            "lease generation must not be empty"
        );
        anyhow::ensure!(
            self.resume_generation
                .as_ref()
                .is_none_or(|g| !g.is_empty()),
            "resume generation must not be empty"
        );
        anyhow::ensure!(
            self.core.require_node_lease,
            "a production actor requires node lease authority"
        );
        anyhow::ensure!(
            self.core.peer_protocol == crate::peer_auth::PROTOCOL_VERSION,
            "actor peer protocol must match the authenticated transport"
        );
        anyhow::ensure!(
            self.core.max_resident > 0
                && self.core.max_activations > 0
                && self.core.max_evictions > 0
                && self.core.max_releases > 0,
            "actor admission and eviction capacities must be positive"
        );
        anyhow::ensure!(
            self.core.idle_evict_ms != Some(0),
            "idle eviction interval must be positive when enabled"
        );
        anyhow::ensure!(
            self.core.owner_log_recovery_attempts == 0
                || self.core.owner_log_recovery_backoff_ms > 0,
            "owner log recovery retries require positive backoff"
        );
        anyhow::ensure!(
            self.core.operation_deadline_ms.is_some_and(|d| d > 0),
            "actor requires a positive operation deadline"
        );
        Ok(())
    }

    pub(super) fn from_environment(
        limits: AdmissionLimits,
        ownership: Option<&Ownership>,
        resume_generation: Option<String>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            core: Config {
                max_resident: limits.resident,
                max_activations: limits.activations,
                #[cfg(not(all(test, cellar_internal_tests)))]
                max_evictions: 4,
                #[cfg(all(test, cellar_internal_tests))]
                max_evictions: limits.evictions,
                max_releases: limits.releases,
                alarm_resident_ms: crate::env_vars::with_default(
                    "CELLAR_ALARM_RESIDENT_MS",
                    3_600_000_i64,
                )?
                .max(0) as u64,
                require_node_lease: true,
                peer_protocol: crate::peer_auth::PROTOCOL_VERSION,
                operation_deadline_ms: Some(crate::env_vars::positive_or(
                    "CELLAR_OPERATION_DEADLINE_MS",
                    super::DEFAULT_OPERATION_DEADLINE_MS,
                )?),
                owner_log_recovery_backoff_ms: crate::env_vars::positive_or(
                    "CELLAR_RECOVERY_RETRY_MS",
                    DEFAULT_OWNER_LOG_RECOVERY_BACKOFF_MS,
                )?,
                owner_log_recovery_attempts: crate::env_vars::positive_or(
                    "CELLAR_RECOVERY_RETRIES",
                    DEFAULT_OWNER_LOG_RECOVERY_ATTEMPTS,
                )?,
                idle_evict_ms: crate::env_vars::positive::<u64>("CELLAR_IDLE_EVICT_S")?
                    .map(|s| s.saturating_mul(1_000)),
                pressure: pressure_config_from_environment()?,
                max_outbound_websockets: crate::env_vars::positive_or(
                    "CELLAR_MAX_OUTBOUND_WEBSOCKETS",
                    DEFAULT_MAX_OUTBOUND_WEBSOCKETS,
                )?,
                ownership_on_evict: match ownership {
                    Some(Ownership::Bucket(_)) => ownership_on_evict_from_environment()?,
                    Some(Ownership::Memory(_)) | None => OwnershipOnEvict::Sticky,
                },
            },
            lease_ttl_ms: crate::env_vars::positive_or("CELLAR_TTL_MS", 10_000)?,
            placement_weight: limits.placement_weight.max(1),
            generation: environment_generation(ownership),
            resume_generation,
        })
    }
}

pub(super) fn environment_generation(ownership: Option<&Ownership>) -> String {
    std::env::var("CELLAR_TEST_GENERATION").unwrap_or_else(|_| {
        match ownership {
            Some(Ownership::Bucket(bucket)) => bucket.process_generation().map(str::to_owned),
            Some(Ownership::Memory(_)) | None => None,
        }
        .unwrap_or_else(random_process_generation)
    })
}
