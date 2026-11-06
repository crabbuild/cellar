// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[test]
fn park_settlement_needs_a_later_entry_or_term() {
    let position = |index, term| LogPosition {
        index: LogIndex::new(index),
        term: Term::new(term),
    };
    assert!(!fences_park(10, 7, position(12, 6)));
    assert!(!fences_park(10, 7, position(10, 7)));
    assert!(!fences_park(10, 7, position(11, 7)));
    assert!(fences_park(10, 7, position(12, 7)));
    assert!(fences_park(10, 7, position(11, 8)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn managed_park_root_refuses_generic_writers_and_reopens_exact_owner() -> anyhow::Result<()> {
    use crate::{asyncrt::ExecutionDomain, bucket::StorageBackend};
    use cellar_logic::consensus::{ClusterId, GroupId, IncarnationId, NamespaceId};
    use object_store::list::{PaginatedListOptions, PaginatedListResult, PaginatedListStore};

    struct NoListing;

    #[async_trait::async_trait]
    impl PaginatedListStore for NoListing {
        async fn list_paginated(
            &self,
            _: Option<&str>,
            _: PaginatedListOptions,
        ) -> object_store::Result<PaginatedListResult> {
            Err(object_store::Error::NotSupported {
                source: "park admission uses exact keys".into(),
            })
        }
    }

    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let memory = Arc::new(object_store::memory::InMemory::new());
    let bucket = Bucket::from_clients(
        memory.clone(),
        memory,
        Arc::new(NoListing),
        StorageBackend::S3,
        "park-catalog".into(),
        "tenant/".into(),
    )?;
    let history = GroupHistory {
        cluster: ClusterId::new([1; 16])?,
        namespace: NamespaceId::new([2; 16])?,
        group: GroupId::new([3; 16])?,
        incarnation: IncarnationId::new([4; 16])?,
    };
    let catalog = FleetCatalog::new(
        &domain,
        bucket,
        history.cluster,
        IncarnationId::new([5; 16])?,
        "nodes/",
    )?;
    let timeout = Duration::from_secs(5);
    let initial = catalog.initialize(timeout).await?;
    let group = catalog.binding(&history)?;
    let record = Record {
        request: FleetRequestIdentity {
            id: [6; 16],
            operation: [7; 16],
            expected_revision: initial.revision(),
            principal: "operator".into(),
        },
        input_root: root_digest(&initial)?,
        group: group.clone(),
        birth: [8; 32],
        schema: [9; 32],
        boot: BootBinding {
            catalog: group.catalog(),
            fleet: group.fleet(),
            node: [10; 16],
            disk: [11; 16],
            engine: [12; 16],
            boot: [13; 16],
            process: [14; 16],
            epoch: 1,
            join_key: ed25519_dalek::SigningKey::from_bytes(&[15; 32])
                .verifying_key()
                .to_bytes(),
        },
        member: 1,
        base: 10,
        term: 7,
        membership: 1,
    };
    let mutation = record.mutation()?;
    assert!(matches!(
        catalog.commit(&initial, &mutation, timeout).await,
        Err(FleetCatalogError::Invalid(
            "managed park operation authority"
        ))
    ));

    // Exercise the actual catalog CAS with an internal, authorized record.
    // The production entry point additionally requires a live quorum sample.
    let committed = catalog
        .run(
            timeout,
            catalog.commit_with_disruption(
                &initial,
                &mutation,
                None,
                Authority::ManagedPark,
                || Ok(()),
            ),
        )
        .await?;
    let operation = ProposalId::new(record.operation())?;
    let reopened = catalog
        .reopen_managed_park(operation, timeout)
        .await?
        .expect("active rooted park");
    assert!(reopened.matches_owner(history, &record.boot, MemberId::new(1)?));
    let request = reopened.prepare_request(100)?;
    assert_eq!(request.proposal, operation);
    assert_eq!(request.expected_applied.index, LogIndex::new(10));
    assert_eq!(request.expected_applied.term, Term::new(7));
    assert_eq!(request.membership.get(), 1);

    let mutate = |table, key: &str| FleetMutation {
        id: [16; 16],
        operation: [17; 16],
        principal: "other".into(),
        expected_revision: committed.snapshot.revision(),
        changes: vec![FleetChange {
            table,
            key: key.into(),
            value: None,
        }],
    };
    for (table, key) in [
        (FleetTable::Policy, SLOT_KEY),
        (FleetTable::Admissions, "membership"),
    ] {
        assert!(matches!(
            catalog.commit(&committed.snapshot, &mutate(table, key), timeout).await,
            Err(FleetCatalogError::DisruptionInProgress { operation }) if operation == record.operation()
        ));
    }
    assert_eq!(
        catalog.read(timeout).await?.revision(),
        committed.snapshot.revision()
    );
    domain.shutdown().await;
    Ok(())
}
