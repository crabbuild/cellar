// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::raft::{
    options::{LogOpenMode, ReplicaLogOptions},
    storage::ReplicaLog,
    version::CellVersionObservation,
};
use cellar_logic::{cell_group::*, consensus::*};

#[tokio::test]
#[allow(clippy::disallowed_methods)]
async fn archived_query_releases_sql_slot_but_retains_its_output_budget() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let node = NodeId::new([1; 16])?;
    let disk = DiskLineage::new([2; 16])?;
    let process = ProcessGeneration::new([3; 16])?;
    let log = domain
        .run(ReplicaLog::open(
            ReplicaLogOptions::new(
                root.path().join("raft"),
                node,
                disk,
                process,
                LogOpenMode::Create,
            ),
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    let group = GroupHistory {
        cluster: ClusterId::new([4; 16])?,
        namespace: NamespaceId::new([5; 16])?,
        group: GroupId::new([6; 16])?,
        incarnation: IncarnationId::new([7; 16])?,
    };
    let observation = CellVersionObservation {
        version: CellVersion {
            history: CellHistory {
                cluster: group.cluster,
                namespace: group.namespace,
                cell: CellId::new("catalog:one")?,
                incarnation: group.incarnation,
            },
            sequence: MutationSequence::new(1)?,
            image: CellImage {
                epoch: 1,
                txid: 1,
                checksum: 1,
                commitment: CellImageCommitment::Materialized([8; 32]),
            },
        },
        witness: CommitWitness {
            group,
            position: LogPosition {
                term: Term::new(1),
                index: LogIndex::new(1),
            },
            proposal: ProposalId::new([9; 16])?,
        },
    };
    let admission = Admission::new(
        CellSqlLimits {
            workers: 1,
            queue_per_worker: 1,
            max_pending_cells: 1,
            group_pending_cells: 1,
            node_bytes: 16_384,
            group_bytes: 16_384,
        },
        log.claim_sql_pool()?,
        1,
        log.performance(),
    );
    let bytes = admission.reserve_read(group, 4096)?;
    let physical = bytes.clone();
    let selected = Box::new(CellSqlQueryResult {
        result: b"selected metadata".to_vec(),
        replica: ReplicaIdentity {
            scope: group,
            node,
            member: MemberId::new(1)?,
            disk,
            process,
        },
        observation: observation.clone(),
        _retention: OutputRetention::Sql { _bytes: bytes },
        _archive: None,
    });
    assert!(matches!(
        admission.reserve_read(group, 4096),
        Err(ExecutionError::Refused(CommandFailure::Overloaded))
    ));
    let archive = Budget::new(16_384, 1);
    let permit = Arc::new(Mutex::new(
        archive.acquire(8192).expect("archive admission"),
    ));
    let selected = selected.retain_for_archive(permit.clone());
    drop(permit);
    // A still-entered physical owner keeps SQL charged even if its waiter left.
    assert_eq!(admission.usage()?, (0, 4096, 1));
    drop(physical);
    // The completed immutable result now belongs to the admitted archive turn.
    // Another discovery read must fit the original one-slot SQL bank.
    let discovery = admission.reserve_read(group, 4096)?;
    assert_eq!(admission.usage()?, (0, 4096, 1));
    assert_eq!(archive.usage(), (8192, 1));
    assert!(
        archive.acquire(1).is_none(),
        "buffered output lost its archive charge"
    );
    assert_eq!(selected.result(), b"selected metadata");
    assert_eq!(selected.observation(), &observation);
    drop(discovery);
    drop(selected);
    assert_eq!(admission.usage()?, (0, 0, 0));
    assert_eq!(archive.usage(), (0, 0));
    drop(admission);
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}
