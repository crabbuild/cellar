// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::daemon::NodeRaftConfig;
use crate::raft::options::{LogOpenMode, ReplicaLogOptions};
use crate::raft::transport_options::TransportOptions;
use cellar_logic::consensus::{DiskLineage, NodeId, ProcessGeneration};

fn config() -> RaftCellsConfig {
    RaftCellsConfig {
        infrastructure: NodeRaftConfig {
            log: ReplicaLogOptions::new(
                "unused-image-policy-test".into(),
                NodeId::new([1; 16]).unwrap(),
                DiskLineage::new([2; 16]).unwrap(),
                ProcessGeneration::new([3; 16]).unwrap(),
                LogOpenMode::Create,
            ),
            consensus: Default::default(),
            transport: Default::default(),
        },
        catalog: Default::default(),
        cells: vec![],
        peers: vec![],
        query_origins: vec![],
        host: Default::default(),
    }
}

#[test]
fn materialized_profiles_cover_worst_case_physical_snapshots() {
    for bytes in [
        64 * 1024 * 1024,
        128 * 1024 * 1024,
        256 * 1024 * 1024,
        10_000_000_000,
    ] {
        let config = config().with_materialized_image_limit(bytes).unwrap();
        config.host.validate().unwrap();
        let transport = &config.infrastructure.transport;
        let snapshot = transport.snapshot_limits(bytes).unwrap();
        snapshot.apply().validate().unwrap();
        let physical = cellar_ltx::ltx::full_snapshot_bound(bytes, 512).unwrap();
        assert!(snapshot.max_encoded_bytes >= physical);
        assert!(config.host.archive.limits.max_snapshot_bytes >= physical as u64);
        assert!(config.infrastructure.log.snapshot_scratch.max_bytes >= 4 * physical as u64);
        assert!(config.host.images.apply.decode.max_decoded_bytes <= 64 * 1024 * 1024);
        assert!(config.host.archive.limits.max_object_bytes <= 128 * 1024 * 1024);
        assert!(config.host.images.max_bytes >= 8 * bytes);
        assert!(
            transport.max_seed_recovery_bytes
                >= config.host.archive.limits.max_set_bytes
                    + super::super::super::archive::RECOVERY_BUNDLE_READ_BYTES
        );
    }
}

#[test]
fn materialized_profile_refuses_unbounded_or_unaligned_images() {
    for bytes in [
        0,
        4095,
        64 * 1024 * 1024 + 1,
        10_000_000_000 + 512,
        u64::MAX,
    ] {
        assert!(config().with_materialized_image_limit(bytes).is_err());
    }
    let default = config();
    assert_eq!(default.host.execution.max_image_bytes, 64 * 1024 * 1024);
    default.host.validate().unwrap();
}

#[test]
fn snapshot_image_quota_is_independent_of_bounded_transfer_memory() {
    let mut transport = TransportOptions {
        max_snapshot_bytes: 512 * 1024 * 1024,
        ..Default::default()
    };
    // A large encoded allowance alone does not authorize a larger logical image.
    transport.validate().unwrap();
    assert!(transport.snapshot_limits(128 * 1024 * 1024).is_err());
    let before = crate::raft::transport::snapshots::memory_bound(&transport);
    transport.max_snapshot_image_bytes = 128 * 1024 * 1024;
    assert!(transport.snapshot_limits(128 * 1024 * 1024).is_ok());
    assert_eq!(
        crate::raft::transport::snapshots::memory_bound(&transport),
        before
    );
}
