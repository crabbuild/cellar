// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::raft::transport::tests::Fixture;
use anyhow::{ensure, Result};
use cellar_ltx::stream_snapshot::IoProfile;

#[tokio::test]
async fn codec_admission_covers_the_selected_profile_and_one_source_owner() -> Result<()> {
    for (maximum, profile) in [
        (64 * 1024 * 1024, IoProfile::Compact),
        (256 * 1024 * 1024 - 1, IoProfile::Compact),
        (256 * 1024 * 1024, IoProfile::Buffered),
        (10_000_000_000, IoProfile::Buffered),
    ] {
        let options = TransportOptions {
            max_snapshot_image_bytes: maximum,
            max_snapshot_bytes: cellar_ltx::stream_snapshot::MAX_ENCODED_BYTES as usize,
            ..Default::default()
        };
        let bytes = memory_bound(&options);
        ensure!(bytes >= 4 * options.snapshot_chunk_bytes + profile.max_working_bytes());
        let fixture = Fixture::start(options, vec![]).await?;
        let state = fixture.transport.handle.0.clone();
        let reservation = fixture.transport.reserve_snapshot_source()?;
        ensure!(reservation.io_profile() == profile);
        ensure!(state.snapshot_source_budget.capacity() == (bytes, 1));
        ensure!(state.snapshot_source_budget.usage() == (bytes, 1));
        ensure!(matches!(
            fixture.transport.reserve_snapshot_source(),
            Err(TransportError::Overloaded)
        ));
        drop(reservation);
        ensure!(state.snapshot_source_budget.usage() == (0, 0));
        // Releasing the exact owner restores admission without a new budget.
        drop(fixture.transport.reserve_snapshot_source()?);
        fixture.close().await?;
    }
    Ok(())
}

#[test]
fn codec_memory_stays_independent_of_large_image_size() {
    let mut options = TransportOptions::default();
    assert_eq!(memory_bound(&options), 8 * 1024 * 1024);
    options.max_snapshot_image_bytes = 256 * 1024 * 1024;
    let large = memory_bound(&options);
    assert_eq!(
        large,
        4 * 1024 * 1024 + IoProfile::Buffered.max_working_bytes()
    );
    options.max_snapshot_image_bytes = 10_000_000_000;
    assert_eq!(memory_bound(&options), large);
}
