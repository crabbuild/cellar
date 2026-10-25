// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub const SCRIPT: &str = "fleetcare";
pub const VERSION: &str = "v1";

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Profile {
    Smoke,
    Capacity,
    Full,
}

#[derive(Clone, Debug, Serialize)]
pub struct Counts {
    pub tenants: usize,
    pub devices: usize,
    pub books: usize,
    pub inventory: usize,
    pub workflows: usize,
    pub sql: usize,
    pub kv: usize,
    pub queues: usize,
    pub cron: usize,
}

impl Counts {
    pub fn total(&self) -> usize {
        self.devices
            + self.books
            + self.inventory
            + self.workflows
            + self.sql
            + self.kv
            + self.queues
            + self.cron
    }

    pub fn in_tenant(&self, total: usize, tenant: usize) -> usize {
        total / self.tenants + usize::from(tenant < total % self.tenants)
    }
}

impl Profile {
    pub fn counts(self) -> Counts {
        match self {
            Self::Full => Counts {
                tenants: 20,
                devices: 5999,
                books: 2400,
                inventory: 1000,
                workflows: 500,
                sql: 60,
                kv: 20,
                queues: 20,
                cron: 1,
            },
            Self::Capacity => Counts {
                tenants: 6,
                devices: 1793,
                books: 720,
                inventory: 300,
                workflows: 156,
                sql: 18,
                kv: 6,
                queues: 6,
                cron: 1,
            },
            Self::Smoke => Counts {
                tenants: 2,
                devices: 51,
                books: 24,
                inventory: 10,
                workflows: 4,
                sql: 6,
                kv: 2,
                queues: 2,
                cron: 1,
            },
        }
    }
}

#[derive(Clone)]
pub struct AppConfig {
    pub profile: Profile,
    pub node: Arc<str>,
    pub token: Arc<str>,
    pub asynchronous: bool,
    pub simulator: Option<Arc<str>>,
    pub container_image: Option<Arc<str>>,
    pub fence_image: Option<Arc<str>>,
    pub offline_ms: i64,
    pub reservation_ms: i64,
}

impl AppConfig {
    /// This is the application's process configuration boundary. SDK library
    /// defaults and per-event application code do not inspect the environment.
    pub fn from_env() -> Result<Self> {
        let profile = match std::env::var("FLEETCARE_PROFILE")
            .as_deref()
            .unwrap_or("full")
        {
            "full" => Profile::Full,
            "smoke" => Profile::Smoke,
            "capacity" => Profile::Capacity,
            _ => anyhow::bail!("FLEETCARE_PROFILE must be full, capacity or smoke"),
        };
        let token: Arc<str> = std::env::var("FLEETCARE_TOKEN")
            .map_err(|_| anyhow::anyhow!("FLEETCARE_TOKEN is required"))?
            .into();
        ensure!(
            token.len() >= 16,
            "application token must have at least 16 characters"
        );
        Ok(Self {
            profile,
            node: std::env::var("CELLAR_NODE")
                .unwrap_or_else(|_| "fleetcare".into())
                .into(),
            token,
            asynchronous: std::env::var("FLEETCARE_ASYNC").as_deref().unwrap_or("1") == "1",
            simulator: std::env::var("FLEETCARE_SIMULATOR").ok().map(Into::into),
            container_image: std::env::var("FLEETCARE_CONTAINER_IMAGE")
                .ok()
                .map(Into::into),
            fence_image: std::env::var("FLEETCARE_FENCE_IMAGE").ok().map(Into::into),
            offline_ms: 300_000,
            reservation_ms: 60_000,
        })
    }

    pub fn validate_target(&self, tenant: usize, kind: &str, id: usize) -> Result<()> {
        let counts = self.profile.counts();
        ensure!(tenant < counts.tenants, "unknown tenant");
        let count = match kind {
            "devices" => counts.devices,
            "books" => counts.books,
            "inventory" => counts.inventory,
            _ => anyhow::bail!("unknown entity class"),
        };
        ensure!(id < counts.in_tenant(count, tenant), "unknown entity");
        Ok(())
    }
}

pub fn binding(prefix: &str, tenant: usize, partition: usize) -> String {
    format!("{prefix}_{tenant}_{partition}")
}

pub fn sql_identity(tenant: usize, partition: usize) -> String {
    format!("fleetcare-tenant-{tenant}-report-{partition}")
}

pub fn kv_identity(tenant: usize) -> String {
    format!("fleetcare-tenant-{tenant}-policy")
}

pub fn queue_identity(pair: usize, dead: bool) -> String {
    format!(
        "fleetcare-queue-{pair}-{}",
        if dead { "dead" } else { "main" }
    )
}

pub fn workflow_id(tenant: usize, slot: usize) -> String {
    format!("tenant-{tenant}-dispatcher-{slot}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inventories_include_system_authorities() {
        assert_eq!(Profile::Full.counts().total(), 10_000);
        assert_eq!(Profile::Smoke.counts().total(), 100);
        let counts = Profile::Full.counts();
        assert_eq!(counts.in_tenant(counts.devices, 0), 300);
        assert_eq!(counts.in_tenant(counts.devices, 19), 299);
        assert_eq!(Profile::Smoke.counts().queues, 2);
    }

    #[test]
    fn capacity_inventory_has_exactly_3000_roots_and_complete_partitions() {
        let counts = Profile::Capacity.counts();
        assert_eq!(counts.total(), 3000);
        assert_eq!(counts.sql, counts.tenants * 3);
        assert_eq!(counts.workflows % counts.tenants, 0);
        assert_eq!(counts.queues % 2, 0);
        for total in [counts.devices, counts.books, counts.inventory] {
            assert_eq!(
                (0..counts.tenants)
                    .map(|t| counts.in_tenant(total, t))
                    .sum::<usize>(),
                total
            );
        }
    }
}
