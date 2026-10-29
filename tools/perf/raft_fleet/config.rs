// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit deployment inputs. Platform measurements are supplied separately.
use super::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub incarnation: [u8; 16],
    pub physical_host: [u8; 16],
    pub owner: String,
    pub nodes: u8,
    pub group_capacity: u32,
    pub image_bytes: u64,
    pub extent_bytes: u64,
    pub replay_extent_bytes: u64,
    pub durable_bytes: u64,
    pub storage: StorageBounds,
    pub resources: FleetDockerResourcePolicy,
}

/// Hard filesystem ceilings installed by the dedicated Colima deployment.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageBounds {
    pub metadata_bytes: u64,
    pub raft_bytes: u64,
    pub images_bytes: u64,
    pub replay_bytes: u64,
    pub object_store_bytes: u64,
    pub snapshot_scratch_bytes: u64,
    pub extent_cache_bytes: u64,
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        IncarnationId::new(self.incarnation)?;
        IncarnationId::new(self.physical_host)?;
        ensure!(
            (3..=5).contains(&self.nodes),
            "managed lab requires three to five nodes"
        );
        ensure!(
            (1..=4096).contains(&self.group_capacity),
            "invalid group capacity"
        );
        ensure!(
            (1 << 20..=64 << 20).contains(&self.image_bytes)
                && self.image_bytes.is_multiple_of(4096),
            "invalid image quota"
        );
        ensure!(
            !self.owner.is_empty()
                && self.owner.len() <= 64
                && self
                    .owner
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-'),
            "invalid deployment owner"
        );
        let charge = self.image_bytes * 8;
        ensure!(
            self.extent_bytes >= charge && self.replay_extent_bytes >= charge,
            "extent inventories cannot cover image work"
        );
        let workspace = cellar::sdk::consensus::verification::RootPrivateOptions::workspace_bounds(
            cellar::sdk::consensus::verification::ExecutionLimits {
                max_image_bytes: self.image_bytes,
                ..Default::default()
            },
        )?
        .reservation()?;
        ensure!(
            self.extent_bytes >= workspace + 4 * self.image_bytes,
            "extent inventory cannot cover native WAL, capture and image ownership"
        );
        let storage = &self.storage;
        ensure!(
            [
                storage.metadata_bytes,
                storage.raft_bytes,
                storage.images_bytes,
                storage.replay_bytes,
                storage.object_store_bytes,
                storage.snapshot_scratch_bytes,
                storage.extent_cache_bytes
            ]
            .iter()
            .all(|n| (64 << 20..=64 << 30).contains(n) && n.is_multiple_of(1 << 20)),
            "invalid storage bound"
        );
        ensure!(
            storage.snapshot_scratch_bytes >= charge
                && storage.snapshot_scratch_bytes * 2 <= storage.raft_bytes,
            "snapshot scratch cannot cover image work and shared-log headroom"
        );
        let image_work = (charge * u64::from(self.group_capacity))
            .checked_add(self.extent_bytes)
            .and_then(|n| n.checked_add(charge))
            .and_then(|n| n.checked_add(storage.extent_cache_bytes))
            .context("image storage overflow")?;
        ensure!(
            image_work <= storage.images_bytes * 9 / 10,
            "image filesystem cannot cover images, extents and archive replay"
        );
        ensure!(
            self.replay_extent_bytes
                .checked_add(charge)
                .is_some_and(|n| n <= storage.replay_bytes * 9 / 10),
            "replay filesystem cannot cover its inventories"
        );
        let inventory_overhead = storage.metadata_bytes
            + storage.raft_bytes
            + storage.replay_bytes
            + self.extent_bytes
            + storage.extent_cache_bytes
            + charge
            + (32 << 20);
        ensure!(
            self.resources.durable_overhead_bytes >= inventory_overhead,
            "durable overhead omits bounded storage pools"
        );
        let capacity = storage.metadata_bytes
            + storage.raft_bytes
            + storage.images_bytes
            + storage.replay_bytes;
        ensure!(
            self.durable_bytes <= capacity * 9 / 10,
            "usable disk budget omits filesystem overhead"
        );
        let required = (charge * u64::from(self.group_capacity))
            .checked_add(self.resources.durable_overhead_bytes)
            .context("disk budget overflow")?;
        ensure!(
            self.durable_bytes >= required,
            "disk budget does not cover all group reservations"
        );
        ensure!(
            self.resources.resident.durable_bytes >= charge
                && self.resources.peak.durable_bytes >= self.resources.resident.durable_bytes
                && self.resources.durable_overhead_bytes > 0
                && self.resources.resident.engine_slots == 1
                && self.resources.peak.engine_slots == 1
                && self.resources.resident.memory_bytes > 0
                && self.resources.peak.memory_bytes >= self.resources.resident.memory_bytes
                && self.resources.reserve.engine_slots <= 32_768,
            "resource policy undercharges images"
        );
        Ok(())
    }

    pub fn extent_options(&self) -> RaftExtentOptions {
        let mut options = RaftExtentOptions::default();
        options.inventory.max_bytes = self.extent_bytes;
        options.cache_scratch.max_bytes = self.storage.extent_cache_bytes;
        options
    }

    pub fn request(&self, step: u8, revision: u64) -> FleetRequestIdentity {
        use sha2::{Digest, Sha256};
        let mut hash = Sha256::new();
        hash.update(b"cellar-managed-lab-bootstrap-1");
        hash.update(self.incarnation);
        hash.update([step]);
        let digest = hash.finalize();
        let mut id = [0; 16];
        id.copy_from_slice(&digest[..16]);
        FleetRequestIdentity {
            id,
            operation: self.incarnation,
            principal: self.owner.clone(),
            expected_revision: revision,
        }
    }
}
