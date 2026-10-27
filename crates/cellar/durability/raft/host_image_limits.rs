// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Coherent opt-in bounds for materialized and extent-backed Raft images.
use super::*;

impl RaftCellsConfig {
    /// Configure a fully materialized cell image, including its snapshot,
    /// archive, transfer reservations and generation-store disk admission.
    ///
    /// Defaults remain 64 MiB. The quota accepts up to decimal 10 GB. Full
    /// snapshots use bounded scratch files and object parts; command delta and
    /// decoder limits stay independent. SQLite rounds usable space down to its
    /// page size. Configure quotas identically on all replicas. This policy is
    /// not by itself capacity or recovery-latency qualification.
    pub fn with_materialized_image_limit(mut self, bytes: u64) -> anyhow::Result<Self> {
        self.configure_image_limit(bytes, true)?;
        Ok(self)
    }

    /// Configure an extent-only deployment with the same logical quota and
    /// bounded command limits. Root transfers do not reserve full-image spools;
    /// one shared node inventory admits durable extents and private workspaces.
    /// This does not convert a materialized namespace or qualify recovery time.
    pub fn with_extent_image_limit(
        mut self,
        bytes: u64,
        options: super::RaftExtentOptions,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            self.cells
                .iter()
                .all(|cell| cell.namespace.image_storage == super::RaftImageStorage::Extents),
            "extent image policy requires extent namespaces"
        );
        self.host.extents = Some(options);
        self.configure_image_limit(bytes, false)?;
        Ok(self)
    }

    fn configure_image_limit(
        &mut self,
        bytes: u64,
        materialized_scratch: bool,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            (4096..=cellar_ltx::stream_snapshot::MAX_IMAGE_BYTES).contains(&bytes)
                && bytes.is_multiple_of(512),
            "image quota must be a multiple of 512, at most 10,000,000,000 bytes"
        );
        let encoded = cellar_ltx::ltx::full_snapshot_bound(bytes, 512)
            .ok_or_else(|| anyhow::anyhow!("snapshot size bound overflow"))?;
        let scratch = &mut self.infrastructure.log.snapshot_scratch;
        if materialized_scratch {
            scratch.max_bytes = scratch.max_bytes.max(4 * encoded as u64);
        }
        scratch.validate()?;
        self.host.execution.max_image_bytes = bytes;
        // Leave room for receipts/sequence floors without preallocating pages.
        // A physical file may stay at its high-water mark after cleanup; policy
        // accounts live btree pages so freed pages remain usable at capacity.
        self.host.execution.receipt_reserve_bytes = self
            .host
            .execution
            .receipt_reserve_bytes
            .max((bytes / 16).min(32 * 1024 * 1024) / 512 * 512);
        let apply = &mut self.host.images.apply;
        apply.decode.max_image_bytes = bytes;

        let copies = if self.host.enable_snapshots { 8 } else { 4 };
        self.host.images.max_bytes = self.host.images.max_bytes.max(bytes * copies);
        let archive = &mut self.host.archive;
        archive.limits.max_image_bytes = bytes;
        archive.limits.max_snapshot_bytes = archive.limits.max_snapshot_bytes.max(encoded as u64);
        let reservation = bytes
            + self.host.execution.envelope.max_payload_bytes as u64
            + self.host.execution.envelope.max_header_bytes as u64
            + 1024;
        archive.max_unarchived_bytes = archive.max_unarchived_bytes.max(4 * reservation);

        let transport = &mut self.infrastructure.transport;
        transport.max_snapshot_image_bytes = transport.max_snapshot_image_bytes.max(bytes);
        transport.max_snapshot_bytes = transport.max_snapshot_bytes.max(encoded);
        transport.max_seed_recovery_bytes = transport.max_seed_recovery_bytes.max(
            archive.limits.max_set_bytes
                + super::super::archive::RECOVERY_BUNDLE_READ_BYTES
                + super::super::archive::physical_read_bytes(),
        );
        if bytes > 64 * 1024 * 1024 {
            transport.snapshot_timeout_ms = transport.snapshot_timeout_ms.max(300_000);
        }
        self.host.validate()?;
        transport.validate()?;
        transport.snapshot_limits(bytes)?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "host_image_limits_tests.rs"]
mod tests;
