//! Committed images and confirmed archive cuts are explicit host wakeups.
#![allow(clippy::disallowed_methods)]
use super::public_node;
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{consensus::*, ShutdownMode};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

pub async fn run() -> Result<()> {
    let (gate, _entered) = public_node::Gate::new();
    gate.release();
    let calls = Arc::new(AtomicUsize::new(0));
    let definition = public_node::counter(calls.clone(), gate)?;
    let mut namespace = NamespaceConfig::new(
        ClusterId::new([1; 16])?,
        NamespaceId::new([2; 16])?,
        NamespaceMode::Raft,
        definition.schema,
    );
    namespace.voters = VoterPolicy::SingleVoterDevelopment;
    let registration = RaftCellRegistration {
        placement: RaftCellPlacement::Replica,
        namespace: namespace.clone(),
        class: "Counter".into(),
        birth: BirthProposal {
            scope: GroupScope {
                cluster: namespace.cluster,
                namespace: namespace.namespace,
                cell: "Counter:wakeup".into(),
                group: GroupId::new([10; 16])?,
                incarnation: IncarnationId::new([3; 16])?,
            },
            voters: vec![InitialMember {
                member: MemberId::new(1)?,
                node: NodeId::new([1; 16])?,
            }],
            learners: vec![],
        },
    };
    let root = tempfile::tempdir()?;
    let node = public_node::node(root.path(), definition.application.clone()).await?;
    let service = public_node::cells_with_host(
        &node,
        definition.application.clone(),
        Arc::new(object_store::memory::InMemory::new()),
        registration,
        false,
        RaftHostOptions {
            // Only maintenance/retries are slow. Raft election and physical
            // completion events retain their normal scheduling.
            poll_ms: 1000,
            ..Default::default()
        },
    )
    .await?;
    let result = async {
        let cell = service.cell(namespace.namespace, "Counter:wakeup")?;
        public_node::ready(&cell).await?;
        let started = Instant::now();
        let mut acknowledged = 0;
        // Eight sequential durable commands force eight distinct publications.
        // Four seconds allows ample real file I/O, but cannot accommodate one
        // maintenance interval per command. This is a scheduling regression,
        // not a production latency target or a test of a mocked archive proof.
        let completion = tokio::time::timeout(Duration::from_secs(4), async {
            for sequence in 1..=8 {
                let receipt = cell
                    .command(
                        definition.increment.clone(),
                        1_i64.to_be_bytes().to_vec(),
                        public_node::archived_options(sequence)?,
                    )
                    .await?;
                ensure!(receipt.durability == Durability::S3);
                ensure!(receipt.result == (sequence as i64).to_be_bytes());
                let observed = cell.status().await?;
                ensure!(observed.archived >= receipt.version.position.index);
                acknowledged += 1;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await;
        eprintln!(
            "archive wake probe: {acknowledged}/8 acknowledged in {:?}, {} callbacks",
            started.elapsed(),
            calls.load(Ordering::Acquire)
        );
        completion.context("S3 completion still depends on maintenance polling")??;
        ensure!(calls.load(Ordering::Acquire) == 8);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let closed = service.close().await;
    let stopped = node.shutdown(ShutdownMode::Preserve).await;
    result?;
    ensure!(closed?.unfinished.is_empty());
    stopped?;
    Ok(())
}
