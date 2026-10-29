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
    // One extent plus its quota-bounded metadata now sets the Compact floor.
    assert_eq!(memory_bound(&options), 9 * 1024 * 1024);
    options.max_snapshot_image_bytes = 256 * 1024 * 1024;
    let large = memory_bound(&options);
    assert_eq!(
        large,
        4 * 1024 * 1024 + IoProfile::Buffered.max_working_bytes()
    );
    options.max_snapshot_image_bytes = 10_000_000_000;
    assert_eq!(memory_bound(&options), large);
}

#[test]
fn legacy_snapshot_authentication_is_refused_before_metadata_decoding() -> Result<()> {
    let auth = PeerAuth::new([8; 32], "node-1")?;
    let options = TransportOptions::default();
    let wire = Wire {
        auth: &auth,
        exporter: &[9; 32],
        options: &options,
        max_body: 1024,
    };
    let value = serde_json::json!({"probe": 1});
    for (magic, old, current) in [
        (
            REQUEST_MAGIC,
            b"cellar-snapshot-pull-1".as_slice(),
            REQUEST_PURPOSE,
        ),
        (
            RESPONSE_MAGIC,
            b"cellar-snapshot-chunk-1".as_slice(),
            RESPONSE_PURPOSE,
        ),
    ] {
        let legacy = wire.encode(magic, old, &[], &value, &[])?;
        ensure!(matches!(
            wire.decode::<serde_json::Value>(magic, current, &legacy, None),
            Err(TransportError::Authentication)
        ));
        let current_head = wire.encode(magic, current, &[], &value, &[])?;
        ensure!(wire.decode::<serde_json::Value>(magic, current, &current_head, None)? == value);
    }
    Ok(())
}

#[tokio::test]
async fn prepared_owner_shutdown_joins_even_if_the_stop_waiter_was_never_polled() -> Result<()> {
    let fixture = Fixture::start(TransportOptions::default(), vec![]).await?;
    let cache = fixture.transport.prepared_snapshot_cache();
    // This is the same outside-future guard capture used by start_prepared.
    // Dropping an unpolled observer must wake its blocking mailbox owner.
    let stop = StopPrepared(cache.clone());
    let observer = async move {
        let _stop = stop;
        std::future::pending::<()>().await;
    };
    drop(observer);
    ensure!(cache.trim_idle().is_err());
    fixture.domain.run(fixture.transport.close()).await?;
    ensure!(fixture.transport.usage().tasks == 0);
    fixture.close().await
}

#[tokio::test]
async fn obsolete_snapshot_routes_cancel_sources_without_releasing_live_owners() -> Result<()> {
    use cellar_logic::consensus::{
        ClusterId, DiskLineage, GroupId, IncarnationId, NamespaceId, ProcessGeneration,
    };
    let scope = GroupScope {
        cluster: ClusterId::new([1; 16])?,
        namespace: NamespaceId::new([2; 16])?,
        cell: "Counter:snapshot-route".into(),
        group: GroupId::new([3; 16])?,
        incarnation: IncarnationId::new([4; 16])?,
    };
    let fixture = Fixture::start(
        TransportOptions::default(),
        vec![crate::raft::storage::GroupBootstrap {
            scope: scope.clone(),
            member: MemberId::new(1)?,
            birth_digest: [10; 32],
            voters: vec![MemberId::new(1)?, MemberId::new(2)?],
            learners: vec![],
        }],
    )
    .await?;
    let state = fixture.transport.handle.0.clone();
    let group = state.runtime.log().group(&scope)?;
    // Snapshot descriptors require a committed position beyond genesis.
    let mut write = crate::raft::cache::GroupWrite::new(&group);
    write.entries.push(::raft::prelude::Entry {
        index: 2,
        term: 1,
        ..Default::default()
    });
    write.hard_state = Some(::raft::prelude::HardState {
        term: 1,
        commit: 2,
        ..Default::default()
    });
    write.applied = Some(crate::raft::cache::DurableApply {
        position: cellar_logic::consensus::LogPosition {
            index: cellar_logic::consensus::LogIndex::new(2),
            term: cellar_logic::consensus::Term::new(1),
        },
        image: None,
    });
    state
        .runtime
        .log()
        .persist(cellar_logic::consensus::OperationId::new(1), vec![write])
        .await?;

    let mut route = GroupRoute {
        scope: scope.clone(),
        schema_digest: [9; 32],
        revision: 1,
        peers: vec![
            RaftPeer {
                replica: group.identity().clone(),
                label: "node-1".into(),
                server_name: "localhost".into(),
                control_address: fixture.transport.control_address(),
                data_address: fixture.transport.data_address(),
                query_address: fixture.transport.query_address(),
            },
            RaftPeer {
                replica: ReplicaIdentity {
                    scope: scope.clone(),
                    member: MemberId::new(2)?,
                    node: NodeId::new([2; 16])?,
                    disk: DiskLineage::new([3; 16])?,
                    process: ProcessGeneration::new([4; 16])?,
                },
                label: "node-2".into(),
                server_name: "localhost".into(),
                control_address: "127.0.0.1:42001".parse()?,
                data_address: "127.0.0.1:42002".parse()?,
                query_address: "127.0.0.1:42003".parse()?,
            },
        ],
    };
    fixture.transport.install_group(route.clone())?;
    for remove in [false, true] {
        let package = SnapshotPackage::capture_empty(&group, [9; 32], SnapshotLimits::default())?;
        let lease = fixture.transport.serve_snapshot(
            &scope,
            2,
            package,
            fixture.transport.reserve_snapshot_source()?,
        )?;
        ensure!(!lease.finished(0));
        // An unchanged discovery view must not interrupt a valid transfer.
        fixture.transport.install_group(route.clone())?;
        ensure!(!lease.finished(0));
        let retained = lease.0.clone();
        if remove {
            fixture.transport.remove_group(&scope)?;
        } else {
            route.revision += 1;
            fixture.transport.install_group(route.clone())?;
        }
        ensure!(
            lease.finished(0),
            "obsolete route retained a live source lease"
        );
        drop(lease);
        ensure!(
            state.snapshot_source_budget.usage().1 == 1,
            "a retained source lost its physical charge"
        );
        drop(retained);
        ensure!(state.snapshot_source_budget.usage() == (0, 0));
    }
    fixture.close().await?;
    Ok(())
}
