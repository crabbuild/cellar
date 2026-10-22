//! Exercise preview promotion through the real node SQL owner and output gate.
use super::*;
#[allow(dead_code)]
#[path = "raft_ltx_fault.rs"]
mod fault;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_seals_each_command_image_once_and_recovers_its_receipts() -> Result<()> {
    use cellar::sdk::{consensus::*, ShutdownMode};
    use std::sync::atomic::AtomicUsize;
    let root = tempfile::tempdir()?;
    let (gate, _entered) = public_node::Gate::new();
    gate.release();
    let calls = Arc::new(AtomicUsize::new(0));
    let definition = public_node::counter(calls.clone(), gate)?;
    let faults = fault::Faults::new(root.path().join("unused"), None);
    let node = public_node::node_declared_with_filesystem(
        root.path(),
        definition.application.clone(),
        "raft-host",
        vec![],
        Arc::new(fault::FaultFileSystem {
            faults: faults.clone(),
        }),
    )
    .await?;
    let image_directory = node
        .info()
        .data_dir
        .join("raft-cells")
        .join(hex::encode([2; 16]))
        .join(hex::encode([5; 16]))
        .join(hex::encode([4; 16]))
        .join("images");
    let mut namespace = config(2, NamespaceMode::Raft, true);
    namespace.schema_digest = definition.schema;
    let registration = RaftCellRegistration {
        placement: RaftCellPlacement::Replica,
        namespace,
        class: "Counter".into(),
        birth: proposal(2, "Counter:preview", 5, 1),
    };
    let store = Arc::new(object_store::memory::InMemory::new());
    let service = public_node::cells(
        &node,
        definition.application.clone(),
        store.clone(),
        registration.clone(),
        false,
    )
    .await?;
    let cell = service.cell(NamespaceId::new([2; 16])?, "Counter:preview")?;
    let outcome = async {
        public_node::ready(&cell).await?;
        let mut requests = Vec::new();
        for sequence in 1..=8 {
            let request = public_node::archived_options(sequence)?;
            let receipt = cell
                .command(
                    definition.increment.clone(),
                    1_i64.to_be_bytes().to_vec(),
                    request.clone(),
                )
                .await
                .with_context(|| {
                    format!(
                        "preview fixture command {sequence}; image operations {:?}",
                        faults.events.lock().unwrap()
                    )
                })?;
            ensure!(receipt.result == (sequence as i64).to_be_bytes());
            requests.push(request);
        }
        let seals = faults
            .image_seals
            .lock()
            .unwrap()
            .iter()
            .filter(|path| path.parent() == Some(image_directory.as_path()))
            .count();
        Ok::<_, anyhow::Error>((seals, requests))
    }
    .await;
    ensure!(service.close().await?.unfinished.is_empty());
    node.shutdown(ShutdownMode::Preserve).await?;
    let (seals, requests) = outcome?;
    assert_eq!(
        seals, 8,
        "each exact leader preview must become its committed image without a second seal"
    );

    // Restart after private executor teardown and old-generation reclamation.
    let node = public_node::node(root.path(), definition.application.clone()).await?;
    let service =
        public_node::cells(&node, definition.application, store, registration, true).await?;
    let cell = service.cell(NamespaceId::new([2; 16])?, "Counter:preview")?;
    let outcome = async {
        public_node::ready(&cell).await?;
        for (index, request) in requests.into_iter().enumerate() {
            let receipt = cell
                .command(
                    definition.increment.clone(),
                    1_i64.to_be_bytes().to_vec(),
                    request,
                )
                .await?;
            ensure!(receipt.result == ((index + 1) as i64).to_be_bytes());
        }
        ensure!(
            cell.query(definition.get, vec![], QueryOptions::default())
                .await?
                .result
                == 8_i64.to_be_bytes()
        );
        ensure!(calls.load(Ordering::SeqCst) == 8);
        Ok(())
    }
    .await;
    ensure!(service.close().await?.unfinished.is_empty());
    node.shutdown(ShutdownMode::Preserve).await?;
    outcome
}
