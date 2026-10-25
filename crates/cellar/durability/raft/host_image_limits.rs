// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Coherent opt-in bounds for the fully materialized Raft image path.
use super::*;

impl RaftCellsConfig {
    /// Configure a fully materialized cell image, including its snapshot,
    /// archive, transfer reservations and generation-store disk admission.
    ///
    /// Defaults remain 64 MiB. This implementation accepts up to 256 MiB;
    /// larger images require a different publication/transfer strategy rather
    /// than bypassing the bounded decoder. This policy is not a capacity or
    /// recovery-latency qualification. Configure it identically on replicas.
    pub fn with_materialized_image_limit(mut self, bytes: u64) -> anyhow::Result<Self> {
        anyhow::ensure!(
            (4096..=256 * 1024 * 1024).contains(&bytes) && bytes.is_multiple_of(4096),
            "materialized image limit must be a multiple of 4096, at most 256 MiB"
        );
        let encoded = cellar_ltx::ltx::full_snapshot_bound(bytes, 512)
            .ok_or_else(|| anyhow::anyhow!("snapshot size bound overflow"))?;
        let pages = usize::try_from(bytes / 512)?;
        self.host.execution.max_image_bytes = bytes;
        let apply = &mut self.host.images.apply;
        apply.decode.max_image_bytes = bytes;
        apply.decode.max_encoded_bytes = apply.decode.max_encoded_bytes.max(encoded);
        apply.max_total_encoded_bytes = apply.max_total_encoded_bytes.max(encoded);
        apply.decode.max_decoded_bytes = apply.decode.max_decoded_bytes.max(bytes);
        apply.max_total_decoded_bytes = apply.max_total_decoded_bytes.max(bytes);
        apply.decode.max_pages = apply.decode.max_pages.max(pages);
        apply.max_total_pages = apply.max_total_pages.max(pages);

        let copies = if self.host.enable_snapshots { 8 } else { 4 };
        self.host.images.max_bytes = self.host.images.max_bytes.max(bytes * copies);
        let archive = &mut self.host.archive;
        archive.limits.max_image_bytes = bytes;
        archive.limits.max_object_bytes = archive.limits.max_object_bytes.max(encoded);
        archive.limits.max_set_bytes = archive.limits.max_set_bytes.max(
            encoded
                .checked_mul(2)
                .and_then(|n| n.checked_add(archive.limits.envelope.max_payload_bytes))
                .ok_or_else(|| anyhow::anyhow!("archive size bound overflow"))?,
        );
        let reservation = bytes
            + self.host.execution.envelope.max_payload_bytes as u64
            + self.host.execution.envelope.max_header_bytes as u64
            + 1024;
        archive.max_unarchived_bytes = archive.max_unarchived_bytes.max(4 * reservation);

        let transport = &mut self.infrastructure.transport;
        transport.max_snapshot_image_bytes = transport.max_snapshot_image_bytes.max(bytes);
        transport.max_snapshot_bytes = transport.max_snapshot_bytes.max(encoded);
        transport.max_seed_recovery_bytes = transport
            .max_seed_recovery_bytes
            .max(archive.limits.max_set_bytes + super::super::archive::RECOVERY_BUNDLE_READ_BYTES);
        self.host.validate()?;
        transport.validate()?;
        transport.snapshot_limits(bytes)?;
        Ok(self)
    }
}

#[cfg(test)]
#[path = "host_image_limits_tests.rs"]
mod tests;
