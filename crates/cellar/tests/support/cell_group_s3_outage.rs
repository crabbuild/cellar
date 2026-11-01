// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A complete backend outage bounds Quorum admission without inventing S3 proof.
use super::*;
use raft::Storage as _;
use std::path::PathBuf;

#[path = "cell_group_backend_control.rs"]
mod backend_control;

const ENTRY_LIMIT: usize = 9;
const OFFLINE_COMMANDS: u64 = 6;
const REFUSED_COMMAND: u64 = OFFLINE_COMMANDS + 1;

async fn gateway(
    h: &Harness,
    store: Arc<archive_controller::archive_store::Store>,
) -> Result<CellGroupDispatcher> {
    let catalog = Catalog::new(store, Default::default())?;
    let config = NamespaceConfig::new(
        h.route.history.cluster,
        h.route.history.namespace,
        cellar_logic::NamespaceMode::Raft,
        h.class.schema_digest(),
    );
    let namespace = h
        .fixture
        .cluster
        .domain
        .run(catalog.existing_namespace(config))
        .await??;
    Ok(CellGroupDispatcher::new(
        &h.fixture.cluster.domain,
        RangeDirectory::new(namespace, Default::default())?,
        h.class.clone(),
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
    )?
    .with_s3(&h.controllers[0])?)
}

async fn state(h: &Harness, index: LogIndex, value: u64) -> Result<()> {
    controller::applied(&h.fixture, h.route.history, index).await?;
    let b = history(&h.route, "registered-b")?;
    for runtime in &h.fixture.runtimes {
        for (cell, expected) in [(&h.cell, value), (&b, 0)] {
            let selected = runtime
                .reconciliation_authority(h.route.history, cell.clone())
                .await?;
            assert_eq!(selected.cell().version.sequence.get(), expected + 1);
            let pin = selected.image().clone();
            h.fixture
                .cluster
                .domain
                .blocking(move || dependencies::image_value(pin, expected))?
                .await??;
        }
        assert_eq!(h.controllers[0].usage().quarantined_groups, 0);
    }
    assert_eq!(h.calls.load(Ordering::SeqCst) as u64, value);
    Ok(())
}

async fn initialize_at_capacity(h: &Harness) -> Result<()> {
    let cell = history(&h.route, "outage-new")?;
    let authority = h.fixture.runtimes[0]
        .enrollment_authority(h.route.history, cell.clone())
        .await?;
    let observed = h.initializations.clone();
    let result = h.fixture.appliers[&(0, h.route.history)]
        .0
        .initialize(
            h.fixture.cell_options(&cell).await?,
            authority,
            CellEnrollmentAttempt {
                creation: ProposalId::new([203; 16])?,
                preview: ImageId::new([203; 16])?,
                inputs: cellar::storage::CommandInputs {
                    now_unix_millis: 10,
                    random: [17; 32],
                },
            },
            move |_| {
                observed.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        )
        .await;
    assert!(matches!(
        result,
        Err(ExecutionError::Refused(CommandFailure::Overloaded))
    ));
    assert_eq!(h.initializations.load(Ordering::SeqCst), 0);
    Ok(())
}

async fn run(
    rooted: bool,
    store: Arc<dyn ObjectStore>,
    control_directory: Option<PathBuf>,
) -> Result<()> {
    // This adapter is shared by catalog and archive. In real RustFS mode it
    // never injects an outage: the owning runner actually stops the container.
    let global = archive_controller::archive_store::Store::around(store);
    let control = backend_control::Control::new(global.clone(), control_directory, rooted);
    let mut retention = CellGroupLogRetentionLimits::default();
    retention.group.entries = ENTRY_LIMIT;
    retention.progress_group.entries = 1;
    let h = Box::pin(Harness::configured_retention(
        rooted,
        global.clone(),
        2,
        Durability::Quorum,
        None,
        None,
        Some(retention),
    ))
    .await?;
    let group = h.fixture.cluster.voters[0]
        .log
        .cell_group(&h.route.history)?;
    // This registered fixture enrolls two cells. Genesis is snapshot index one;
    // the retained inventory is one election no-op and two enrollments.
    for voter in &h.fixture.cluster.voters {
        let selected = voter.log.cell_group(&h.route.history)?;
        assert_eq!(selected.first_index()?, 2);
        let entries = selected.entries(
            selected.first_index()?,
            selected.last_index()? + 1,
            None,
            raft::GetEntriesContext::empty(false),
        )?;
        assert_eq!(
            entries.iter().map(|entry| entry.index).collect::<Vec<_>>(),
            vec![2, 3, 4]
        );
        assert!(entries[0].data.is_empty());
        assert!(entries[1..].iter().all(|entry| !entry.data.is_empty()));
        assert_eq!(
            selected.log_retention_usage()?.retained_entries,
            entries.len()
        );
    }
    let warm = h
        .command(1, b"warm", Durability::Quorum, Duration::from_secs(15))
        .await?
        .response()
        .await?;
    let warm_index = warm.receipt().position.index;
    assert_eq!(warm.durability(), Durability::Quorum);
    drop(warm);
    archive_controller::published(&h.controllers[0], h.route.history, warm_index).await?;
    let gateway_store = archive_controller::archive_store::Store::around(global.clone());
    let remote = remote::Remote::start(&h, gateway(&h, gateway_store.clone()).await?).await?;
    // Warm the receiver too. It must still perform fresh discovery after stop.
    let found = remote::command_using(
        &h,
        &remote,
        remote::command_request(&h, 1, b"warm", Durability::Quorum)?,
        CellRoutingLimits {
            timeout: Duration::from_secs(15),
            max_attempts: 4,
        },
    )
    .await?;
    assert_eq!(found.result(), b"warm");
    drop(found);
    assert_eq!(h.calls.load(Ordering::SeqCst), 1);
    let fresh_gets = gateway_store.gets.load(Ordering::SeqCst);
    let probe = ObjectPath::from("backend-probe");
    let probe_bytes = vec![29_u8; 128];
    global
        .put_opts(
            &probe,
            probe_bytes.clone().into(),
            object_store::PutOptions::from(object_store::PutMode::Create),
        )
        .await?;
    let before_checkpoint = group.checkpoint_manifest()?;
    let before_first = group.first_index()?;
    control
        .set_running(&h.fixture.cluster.domain, false)
        .await?;
    let failed_probe = tokio::time::timeout(Duration::from_secs(2), global.get(&probe)).await?;
    assert!(
        failed_probe.is_err(),
        "stopped backend must refuse actual S3 I/O"
    );

    let remote_request = CellCommandRequest {
        id: CommandId::new(ClientId::new([187; 16])?, 1)?,
        contract: h.class.contract("Add").context("Add")?,
        input: b"remote-outage".to_vec(),
        durability: Durability::Quorum,
        retry_until_ms: h.retry_until,
    };
    let mut remote_invocation = h
        .dispatcher
        .command(
            h.cell.clone(),
            remote_request,
            CellRoutingLimits {
                timeout: Duration::from_secs(15),
                max_attempts: 4,
            },
            None,
        )
        .await?;
    remote_invocation.begin().await?;
    let refused = remote_invocation
        .execute_remote(remote.transport(), MemberId::new(1)?)
        .await;
    assert!(
        refused.is_err(),
        "remote reception requires fresh catalog discovery"
    );
    assert!(gateway_store.gets.load(Ordering::SeqCst) > fresh_gets);
    assert_eq!(h.calls.load(Ordering::SeqCst), 1);

    let pending = h
        .command(2, b"archive-wait", Durability::S3, Duration::from_secs(2))
        .await?;
    h.waiting().await?;
    let mut pending = Box::pin(pending.response());
    assert!(
        tokio::time::timeout(Duration::from_millis(30), pending.as_mut())
            .await
            .is_err()
    );
    let mut latest = warm_index;
    for sequence in 3..=OFFLINE_COMMANDS {
        let response = h
            .command(
                sequence,
                b"quorum-offline",
                Durability::Quorum,
                Duration::from_secs(15),
            )
            .await?
            .response()
            .await?;
        assert_eq!(response.durability(), Durability::Quorum);
        assert_eq!(response.receipt().receipt.result(), b"quorum-offline");
        latest = response.receipt().position.index;
        drop(response);
    }
    state(&h, latest, OFFLINE_COMMANDS).await?;
    let usage = group.log_retention_usage()?;
    assert_eq!(
        (usage.retained_entries, usage.reserved_entries),
        (ENTRY_LIMIT, 0)
    );
    let refused = h
        .command(
            REFUSED_COMMAND,
            b"after-ceiling",
            Durability::Quorum,
            Duration::from_secs(15),
        )
        .await?
        .response()
        .await
        .err()
        .context("retention ceiling must refuse new command")?;
    assert!(matches!(
        refused.cause.as_ref(),
        CellDispatchError::Execution(ExecutionError::Refused(CommandFailure::Overloaded))
    ));
    assert_eq!(h.calls.load(Ordering::SeqCst) as u64, OFFLINE_COMMANDS);
    initialize_at_capacity(&h).await?;
    let known = h
        .command(1, b"warm", Durability::Quorum, Duration::from_secs(15))
        .await?
        .response()
        .await?;
    assert_eq!(known.durability(), Durability::Quorum);
    assert_eq!(known.receipt().receipt.result(), b"warm");
    drop(known);
    let error = pending
        .await
        .err()
        .context("S3 output cannot settle during full outage")?;
    assert!(error.ambiguous);
    assert!(matches!(
        error.cause.as_ref(),
        CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)
    ));
    assert_eq!(h.calls.load(Ordering::SeqCst) as u64, OFFLINE_COMMANDS);
    assert!(
        !h.controllers[0]
            .archive_status(h.route.history)?
            .context("archive status")?
            .stopped
    );
    controller::wait_for(async || {
        Ok(h.controllers[0].usage().retained_s3 == 0
            && h.controllers[0].usage().retained_commands == 0)
    })
    .await?;
    let downgraded = h
        .command(
            2,
            b"archive-wait",
            Durability::Quorum,
            Duration::from_millis(800),
        )
        .await?
        .response()
        .await
        .err()
        .context("stored S3 minimum survives caller downgrade")?;
    assert!(downgraded.ambiguous);
    assert!(matches!(
        downgraded.cause.as_ref(),
        CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)
    ));
    assert_eq!(h.calls.load(Ordering::SeqCst) as u64, OFFLINE_COMMANDS);

    let checkpoints = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let source = h.fixture.runtimes[0]
        .capture_checkpoint(h.route.history, &checkpoints)
        .await?;
    let prepared = h
        .fixture
        .cluster
        .domain
        .blocking(move || source.prepare_publication())?
        .await??;
    let cut = prepared.marker().clone();
    assert_eq!(cut.position().index, latest);
    controller::wait_for(async || {
        match h.fixture.runtimes[0]
            .publish_checkpoint(h.route.history, prepared.clone())
            .await
        {
            Err(RuntimeError::Overloaded) => Ok(true),
            Err(RuntimeError::NotReady) => Ok(false),
            other => anyhow::bail!("uncovered checkpoint must refuse: {other:?}"),
        }
    })
    .await?;
    assert_eq!(group.checkpoint_manifest()?, before_checkpoint);
    assert_eq!(group.first_index()?, before_first);
    assert_eq!(group.log_retention_usage()?.retained_entries, ENTRY_LIMIT);

    control.set_running(&h.fixture.cluster.domain, true).await?;
    assert_eq!(
        global.get(&probe).await?.bytes().await?.as_ref(),
        probe_bytes
    );
    archive_controller::published(&h.controllers[0], h.route.history, latest).await?;
    let coverage =
        archive_controller::verified_current(&h.fixture, &h.archive, rooted, "outage-recovery")
            .await?;
    assert!(coverage.marker().position().index >= latest);
    let image = coverage
        .recovery()
        .manifest()
        .pin_cell(&h.cell.cell)
        .context("archived A")?
        .1;
    h.fixture
        .cluster
        .domain
        .blocking(move || dependencies::image_value(image, OFFLINE_COMMANDS))?
        .await??;
    let image = coverage
        .recovery()
        .manifest()
        .pin_cell(&CellId::new("registered-b")?)
        .context("archived B")?
        .1;
    h.fixture
        .cluster
        .domain
        .blocking(move || dependencies::image_value(image, 0))?
        .await??;
    // Release the completed receiver before another S3 output uses that same
    // unchanged one-source archive pool.
    drop(coverage);
    let recovered = h
        .command(
            2,
            b"archive-wait",
            Durability::Quorum,
            Duration::from_secs(15),
        )
        .await?
        .response()
        .await?;
    assert_eq!(recovered.durability(), Durability::S3);
    assert_eq!(recovered.receipt().receipt.result(), b"archive-wait");
    drop(recovered);
    assert_eq!(h.calls.load(Ordering::SeqCst) as u64, OFFLINE_COMMANDS);

    h.fixture.runtimes[0]
        .publish_checkpoint(h.route.history, prepared)
        .await?;
    for node in 0..3 {
        if node != 0 {
            controller::wait_for(async || Ok(checkpoints.usage().sources == 0)).await?;
            let source = h.fixture.runtimes[node]
                .capture_checkpoint(h.route.history, &checkpoints)
                .await?;
            let prepared = h
                .fixture
                .cluster
                .domain
                .blocking(move || source.prepare_publication())?
                .await??;
            assert_eq!(prepared.marker(), &cut);
            h.fixture.runtimes[node]
                .publish_checkpoint(h.route.history, prepared)
                .await?;
        }
        let log = h.fixture.cluster.voters[node]
            .log
            .cell_group(&h.route.history)?;
        controller::wait_for(async || {
            Ok(log.checkpoint_manifest()?.as_ref() == Some(&cut)
                && log.log_retention_usage()?.retained_entries == 0
                && checkpoints.usage().sources == 0)
        })
        .await?;
    }
    let resumed = h
        .command(
            REFUSED_COMMAND,
            b"after-ceiling",
            Durability::Quorum,
            Duration::from_secs(15),
        )
        .await?
        .response()
        .await?;
    assert_eq!(resumed.durability(), Durability::Quorum);
    drop(resumed);
    // Preserve the same invocation, captured inputs, original deadline and
    // attempt budget after the failed remote response.
    remote_invocation.retry().await?;
    let resumed = remote_invocation
        .execute_remote(remote.transport(), MemberId::new(1)?)
        .await?;
    assert_eq!(resumed.durability(), Durability::Quorum);
    assert_eq!(resumed.result(), b"remote-outage");
    drop(resumed);
    drop(remote_invocation);
    latest = h.fixture.runtimes[0]
        .status(h.route.history)
        .await?
        .applied
        .index;
    state(&h, latest, OFFLINE_COMMANDS + 2).await?;
    archive_controller::published(&h.controllers[0], h.route.history, latest).await?;
    remote.close().await?;
    drop(group);
    h.close().await
}

#[inline(never)]
pub(crate) fn full_backend_outage(
    rooted: bool,
    store: Arc<dyn ObjectStore>,
    control_directory: Option<PathBuf>,
) -> std::pin::Pin<Box<impl std::future::Future<Output = Result<()>>>> {
    Box::pin(run(rooted, store, control_directory))
}
