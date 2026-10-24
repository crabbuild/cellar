// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Aggregate log pressure must compact before individual entry-count triggers.
use super::*;
use cellar::sdk::{consensus::*, NodeRaftConfig, NodeRaftListeners, ShutdownMode};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_small_groups_checkpoint_under_shared_cache_pressure() -> Result<()> {
    let (gate, _entered) = public_node::Gate::new();
    gate.release();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let definition = public_node::counter(calls.clone(), gate)?;
    let root = tempfile::tempdir()?;
    let node = public_node::node(root.path(), definition.application.clone()).await?;
    let mut namespace = config(2, NamespaceMode::Raft, true);
    namespace.schema_digest = definition.schema;
    let registrations = (0..8)
        .map(|id| RaftCellRegistration {
            placement: RaftCellPlacement::Replica,
            namespace: namespace.clone(),
            class: "Counter".into(),
            birth: proposal(2, &format!("Counter:cache-{id}"), id + 10, 1),
        })
        .collect();
    let mut log = ReplicaLogOptions::new(
        node.info().data_dir.join("raft"),
        NodeId::new([1; 16])?,
        DiskLineage::new([10; 16])?,
        ProcessGeneration::new([11; 16])?,
        LogOpenMode::Create,
    );
    log.max_cache_bytes = 256 * 1024;
    let service = node
        .start_raft_cells(
            RaftCellsConfig {
                query_origins: vec![],
                infrastructure: NodeRaftConfig {
                    log,
                    consensus: RaftRuntimeOptions {
                        tick_interval_ms: Some(10),
                        ..Default::default()
                    },
                    transport: TransportOptions::default(),
                },
                catalog: CatalogOptions {
                    prefix: "host/cache".into(),
                    ..Default::default()
                },
                cells: registrations,
                peers: vec![],
                host: RaftHostOptions::default(),
            },
            RaftCellsServices {
                listeners: NodeRaftListeners {
                    control: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
                    data: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
                    query: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
                },
                tls: public_node::tls()?,
                store: Arc::new(object_store::memory::InMemory::new()),
                application: definition.application.clone(),
                versions: public_node::version_keys()?,
            },
        )
        .await?;
    let result = async {
        let cells = (0..8)
            .map(|id| service.cell(namespace.namespace, &format!("Counter:cache-{id}")))
            .collect::<Result<Vec<_>, _>>()?;
        for cell in &cells {
            public_node::ready(cell).await?;
        }
        let mut last = vec![None; cells.len()];
        // Every group stays below the default 256-entry checkpoint trigger,
        // while their cumulative command envelopes exceed the shared cache.
        for sequence in 1..=32 {
            for (id, cell) in cells.iter().enumerate() {
                public_node::ready(cell).await?;
                let options = public_node::options(sequence)?;
                let receipt = tokio::time::timeout(Duration::from_secs(10), async {
                    loop {
                        match cell
                            .command(
                                definition.increment.clone(),
                                1i64.to_be_bytes().to_vec(),
                                options.clone(),
                            )
                            .await
                        {
                            Ok(receipt) => return Ok::<_, CommandFailure>(receipt),
                            Err(CommandFailure::Unavailable | CommandFailure::Overloaded) => {
                                tokio::time::sleep(Duration::from_millis(10)).await
                            }
                            Err(error) => return Err(error),
                        }
                    }
                })
                .await
                .with_context(|| format!("group {id}, sequence {sequence} admission"))?
                .with_context(|| format!("group {id}, sequence {sequence}"))?;
                assert_eq!(receipt.result, (sequence as i64).to_be_bytes());
                last[id] = Some(receipt);
            }
        }
        let mut checkpoints = 0;
        for (id, cell) in cells.iter().enumerate() {
            let status = cell.status().await?;
            checkpoints += usize::from(status.checkpoint.index.get() > 1);
            let value = cell
                .query(
                    definition.get.clone(),
                    vec![],
                    QueryOptions {
                        consistency: QueryConsistency::AtLeast(
                            last[id].as_ref().unwrap().version_token.clone(),
                        ),
                        ..Default::default()
                    },
                )
                .await?;
            assert_eq!(value.result, 32i64.to_be_bytes());
        }
        ensure!(
            checkpoints > 0,
            "cache pressure must trigger physical checkpoints"
        );
        assert_eq!(calls.load(Ordering::Acquire), 256);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let closed = service.close().await;
    node.shutdown(ShutdownMode::Preserve).await?;
    result?;
    ensure!(closed?.unfinished.is_empty());
    Ok(())
}
