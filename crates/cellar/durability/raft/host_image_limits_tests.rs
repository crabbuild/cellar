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
        assert!(config.host.execution.receipt_reserve_bytes > 0);
        assert!(config.host.execution.receipt_reserve_bytes <= bytes / 16);
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

#[test]
fn extent_profiles_keep_full_image_spools_out_of_scratch_admission() {
    for bytes in [64 * 1024 * 1024, 256 * 1024 * 1024, 10_000_000_000] {
        let initial = config();
        let scratch_bytes = initial.infrastructure.log.snapshot_scratch.max_bytes;
        let scratch_files = initial.infrastructure.log.snapshot_scratch.max_files;
        let config = initial
            .with_extent_image_limit(bytes, RaftExtentOptions::default())
            .unwrap();
        config.host.validate().unwrap();
        assert_eq!(config.host.execution.max_image_bytes, bytes);
        assert_eq!(config.host.images.apply.decode.max_image_bytes, bytes);
        assert_eq!(config.host.archive.limits.max_image_bytes, bytes);
        assert_eq!(
            config.infrastructure.log.snapshot_scratch.max_bytes,
            scratch_bytes
        );
        assert_eq!(
            config.infrastructure.log.snapshot_scratch.max_files,
            scratch_files
        );
        assert!(config.host.execution.envelope.max_payload_bytes <= 64 * 1024 * 1024);
        assert!(config.host.images.apply.decode.max_decoded_bytes <= 64 * 1024 * 1024);
        let physical = config.host.extents.unwrap();
        let workspace = RootPrivateOptions::workspace_bounds(config.host.execution)
            .unwrap()
            .reservation()
            .unwrap();
        assert!(physical.inventory.max_bytes >= workspace + 4 * bytes);
        config
            .infrastructure
            .transport
            .snapshot_limits(bytes)
            .unwrap();
    }
}

#[test]
fn extent_profile_refuses_inadequate_shared_inventory() {
    let small = RaftExtentOptions {
        inventory: RaftExtentLimits {
            max_bytes: 10_000_000_000,
            ..Default::default()
        },
        ..Default::default()
    };
    assert!(config()
        .with_extent_image_limit(10_000_000_000, small)
        .is_err());
    let few_objects = RaftExtentOptions {
        inventory: RaftExtentLimits {
            max_objects: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    assert!(config()
        .with_extent_image_limit(10_000_000_000, few_objects)
        .is_err());
    assert!(config()
        .with_extent_image_limit(10_000_000_512, Default::default())
        .is_err());
}

#[test]
fn extent_profile_requires_an_explicit_namespace_choice() {
    let mut configured = config();
    let scope = GroupScope {
        cluster: ClusterId::new([1; 16]).unwrap(),
        namespace: NamespaceId::new([2; 16]).unwrap(),
        cell: "Counter:policy".into(),
        group: GroupId::new([4; 16]).unwrap(),
        incarnation: IncarnationId::new([5; 16]).unwrap(),
    };
    configured.cells.push(RaftCellRegistration {
        placement: RaftCellPlacement::Replica,
        namespace: NamespaceConfig::new(
            scope.cluster,
            scope.namespace,
            cellar_logic::NamespaceMode::Raft,
            [6; 32],
        ),
        class: "Counter".into(),
        birth: BirthProposal {
            scope,
            voters: vec![InitialMember {
                member: MemberId::new(1).unwrap(),
                node: NodeId::new([1; 16]).unwrap(),
            }],
            learners: vec![],
        },
    });
    assert!(configured
        .with_extent_image_limit(10_000_000_000, Default::default())
        .is_err());
}
