// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A reachable S3 backend cannot substitute for an exact output durability proof.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(crate) async fn unproved_s3_responses(
    rooted: bool,
    store: Arc<dyn object_store::ObjectStore>,
) -> Result<()> {
    let callbacks = Arc::new(AtomicUsize::new(0));
    let initializations = callbacks.clone();
    let commands = callbacks.clone();
    let class = Arc::new(
        RaftClass::new("Counter", 1, [81; 32])?
            .initializer([82; 32], move |_, _| {
                initializations.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })?
            .command("Add", [83; 32], move |_, _| {
                commands.fetch_add(1, Ordering::SeqCst);
                Ok(b"unproved".to_vec())
            })?,
    );
    let (fixture, _, cell, _) =
        Box::pin(registered::setup_store(rooted, &class, store.clone())).await?;
    let mut minimum = NamespaceConfig::new(
        cell.cluster,
        cell.namespace,
        cellar_logic::NamespaceMode::Raft,
        class.schema_digest(),
    );
    minimum.minimum_durability = Durability::S3;
    let catalog = Catalog::new(
        Arc::new(object_store::prefix::PrefixStore::new(store, "s3-minimum")),
        Default::default(),
    )?;
    let namespace = fixture
        .cluster
        .domain
        .run(catalog.open_namespace(minimum))
        .await??;
    let required = RangeDirectory::new(namespace, Default::default())?;
    for (directory, durability) in [
        (fixture.cluster.directory.clone(), Durability::S3),
        (required, Durability::Quorum),
    ] {
        let dispatcher = CellGroupDispatcher::new(
            &fixture.cluster.domain,
            directory,
            class.clone(),
            VersionKeys::new(
                VersionSigningKey::new([89; 32], u64::MAX)?,
                vec![],
                Duration::from_secs(30),
            )?,
            CellDispatchLimits {
                max_cached_routes: 32,
                route_cache_bytes: 256 * 1024,
                max_invocations: 8,
                retained_bytes: 1024 * 1024,
                max_input_bytes: 1024,
            },
        )?;
        let bounds = CellRoutingLimits {
            timeout: Duration::from_secs(5),
            max_attempts: 2,
        };
        let id = CommandId::new(ClientId::new([188; 16])?, 1)?;
        let contract = class.contract("Add").context("Add")?;
        let error = dispatcher
            .command(
                cell.clone(),
                CellCommandRequest {
                    id,
                    contract: contract.clone(),
                    input: vec![],
                    durability,
                    retry_until_ms: u64::MAX,
                },
                bounds,
                None,
            )
            .await
            .err()
            .context("unproved S3 command refused")?;
        assert!(!error.ambiguous);
        refusal(&error.cause);
        let error = dispatcher
            .lookup_receipt(
                cell.clone(),
                CellReceiptRequest {
                    id,
                    contract,
                    input: vec![],
                    durability,
                },
                bounds,
                None,
            )
            .err()
            .context("unproved S3 receipt refused")?;
        refusal(&error);
        let error = dispatcher
            .enroll(
                cell.clone(),
                CellEnrollmentRequest {
                    creation: ProposalId::new([188; 16])?,
                    durability,
                },
                bounds,
                None,
            )
            .await
            .err()
            .context("unproved S3 enrollment refused")?;
        assert!(!error.ambiguous);
        refusal(&error.cause);
        assert_eq!(dispatcher.usage(), (0, 0));
    }
    assert_eq!(callbacks.load(Ordering::SeqCst), 0);
    fixture.close().await
}

fn refusal(error: &CellDispatchError) {
    assert!(matches!(
        error,
        CellDispatchError::Execution(ExecutionError::Refused(
            CommandFailure::UnsupportedCapability
        ))
    ));
}
