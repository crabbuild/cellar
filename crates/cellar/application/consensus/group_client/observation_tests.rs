// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::raft::cell_groups::{
    CellGroupArchiveControlError, CellGroupArchiveError, GroupManifest,
};
use cellar_logic::{cell_group::*, consensus::*};

fn route() -> GroupRoute {
    GroupRoute {
        history: GroupHistory {
            cluster: ClusterId::new([1; 16]).unwrap(),
            namespace: NamespaceId::new([2; 16]).unwrap(),
            group: GroupId::new([3; 16]).unwrap(),
            incarnation: IncarnationId::new([4; 16]).unwrap(),
        },
        range: CellRange::all(),
        epoch: RouteEpoch::new(1).unwrap(),
        birth_digest: [5; 32],
        schema_digest: [6; 32],
    }
}

#[test]
fn retryable_upload_failure_preserves_last_complete_partition_lag() {
    let route = route();
    let status = CellGroupArchiveStatus {
        published: Some(GroupManifest::genesis(route.clone()).unwrap()),
        attempts: 17,
        error: Some(Arc::new(CellGroupArchiveControlError::Archive(
            CellGroupArchiveError::Expired,
        ))),
        stopped: false,
    };
    assert_eq!(
        partition_archive_lag(&route, LogIndex::new(5), Some(status.clone())),
        Some(4)
    );
    // A publication newer than the earlier Raft sample is zero lag at that cut,
    // not unavailable evidence. Neither result is a retained archive proof.
    assert_eq!(
        partition_archive_lag(&route, LogIndex::new(0), Some(status)),
        Some(0)
    );
}

#[test]
fn partition_lag_refuses_stopped_missing_or_foreign_archive_owners() {
    let route = route();
    let mut status = CellGroupArchiveStatus {
        published: Some(GroupManifest::genesis(route.clone()).unwrap()),
        ..Default::default()
    };
    let mut foreign = route.clone();
    foreign.epoch = RouteEpoch::new(2).unwrap();
    assert_eq!(
        partition_archive_lag(&foreign, LogIndex::new(5), Some(status.clone())),
        None
    );
    status.stopped = true;
    assert_eq!(
        partition_archive_lag(&route, LogIndex::new(5), Some(status)),
        None
    );
    assert_eq!(
        partition_archive_lag(&route, LogIndex::new(5), Some(Default::default())),
        None
    );
    assert_eq!(partition_archive_lag(&route, LogIndex::new(5), None), None);
}
