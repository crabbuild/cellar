// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Original waiter expiry and real cleanup failure keep their physical owner.

use super::*;
use cellar::sdk::{Node, NodeOutcome, ShutdownMode};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, io};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Cut {
    WaiterDeadline,
    IoFailure,
}

impl Cut {
    fn name(self) -> &'static str {
        match self {
            Self::WaiterDeadline => "waiter-deadline",
            Self::IoFailure => "io-failure",
        }
    }
}

#[derive(Default, serde::Serialize)]
struct Observed {
    held_after_deadline: bool,
    receipt_verified: bool,
    close_waited_for_cleanup: Option<bool>,
    quarantined: bool,
    full_workspace_charge_retained: bool,
    concrete_bytes_retained: bool,
    concrete_bytes_after_join: bool,
}

pub(super) async fn qualify_cancel() -> Result<()> {
    qualify(Cut::WaiterDeadline).await
}

pub(super) async fn qualify_failure() -> Result<()> {
    qualify(Cut::IoFailure).await
}

async fn qualify(cut: Cut) -> Result<()> {
    let mut failures = Vec::new();
    for rooted in [false, true] {
        for durability in [Durability::Quorum, Durability::S3] {
            let store = Arc::new(catalog_fault::Store::default());
            let result = run(rooted, durability, cut, store.clone()).await;
            if let Err(error) = managed_fixture::memory::preserve_result(
                &store.inner,
                "private-cleanup-lifecycle",
                result,
            )
            .await
            {
                failures.push(format!(
                    "rooted={rooted} durability={durability:?} cut={}: {error:#}",
                    cut.name()
                ));
            }
        }
    }
    ensure!(
        failures.is_empty(),
        "private cleanup lifecycle: {failures:?}"
    );
    Ok(())
}

async fn start(
    rooted: bool,
    cut: Cut,
    store: Arc<catalog_fault::Store>,
    control: Arc<Control>,
    injected: Arc<AtomicBool>,
) -> Result<ManagedCluster> {
    let factory = control.clone();
    let class = RaftClass::new("Counter", 1, [224; 32])?
        .initializer([225; 32], |_, tx| {
            tx.storage()?.put_json("value", &json!(0))
        })?
        .json_cell_rpc("Run", [226; 32])?
        .query("Get", [227; 32], |query, _| {
            Ok(serde_json::to_vec(&query.storage().get_json("value")?)?)
        })?;
    let application = Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_cell(
            class,
            move |context, _| {
                Ok(Arc::new(Counter {
                    storage: context.storage()?,
                    control: factory.clone(),
                }))
            },
        )?,
    );
    let declared = configuration::options("private-cleanup-service");
    application.validate_deployment(&configuration::deployment(declared.clone()))?;
    let mut fixture = Fixture::with_application(rooted, "Counter", application, store).await?;
    fixture.configuration = Some(declared);
    fixture.sql_workers = 1;
    let faults = physical::Faults::new("unused-private-cleanup-selection".into(), None);
    let opening = control.clone();
    *faults.lock_open.lock().unwrap() = Some(Arc::new(move |path| {
        if path.file_name().is_some_and(|name| name == "working.lock")
            && opening
                .prefix
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|prefix| path.starts_with(prefix))
            && !opening.armed.load(Ordering::Acquire)
        {
            *opening.directory.lock().unwrap() = path.parent().map(PathBuf::from);
        }
        Ok(())
    }));
    *faults.private_cleanup.lock().unwrap() = Some(Arc::new(move |path| {
        let selected = control
            .directory
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|directory| {
                if rooted {
                    path == directory
                } else {
                    path == directory.join("working.sqlite")
                }
            });
        if selected && control.armed.swap(false, Ordering::AcqRel) {
            control.hold();
            if cut == Cut::IoFailure {
                injected.store(true, Ordering::Release);
                return Err(io::Error::other("injected native private cleanup failure"));
            }
        }
        Ok(())
    }));
    fixture.filesystem = Arc::new(physical::FaultFileSystem { faults });
    let domain = fixture.domain.clone();
    let result = async {
        let mut roots = Vec::new();
        for _ in 0..3 {
            let mut root = tempfile::Builder::new()
                .prefix("cellar-private-cleanup-lifecycle-")
                .tempdir()?;
            root.disable_cleanup(true);
            eprintln!("retained private cleanup disk: {}", root.path().display());
            roots.push(root);
        }
        Box::pin(ManagedCluster::start_with_roots(fixture, roots)).await
    }
    .await;
    if result.is_err() {
        domain.shutdown().await;
    }
    result
}

type Media = BTreeMap<PathBuf, (u64, [u8; 32])>;

async fn media(
    control: &Control,
    domain: &cellar::sdk::ExecutionDomain,
    filesystem: Arc<dyn cellar_ltx::FileSystem>,
) -> Result<Media> {
    let root = control
        .directory
        .lock()
        .unwrap()
        .clone()
        .context("selected private directory")?;
    domain
        .blocking(move || -> Result<Media> {
            let mut files = BTreeMap::new();
            let mut pending = vec![root.clone()];
            while let Some(directory) = pending.pop() {
                for entry in filesystem.read_dir_iter(&directory)? {
                    let entry = entry?;
                    let kind = filesystem.symlink_metadata(&entry.path)?;
                    ensure!(
                        kind.is_dir || kind.is_file,
                        "private fixture contains a symlink or special file"
                    );
                    if kind.is_dir {
                        pending.push(entry.path);
                    } else {
                        ensure!(
                            files.len() < 4096,
                            "private fixture inventory exceeds diagnostic bound"
                        );
                        let bytes = filesystem.read(&entry.path)?;
                        files.insert(
                            entry.path.strip_prefix(&root)?.to_path_buf(),
                            (bytes.len() as u64, Sha256::digest(&bytes).into()),
                        );
                    }
                }
            }
            ensure!(
                files.values().any(|(length, _)| *length > 0),
                "selected cleanup held no concrete bytes"
            );
            Ok(files)
        })?
        .await?
}

fn held(host: &RaftCells, rooted: bool) -> Result<Option<RaftExtentUsage>> {
    let usage = host.resource_usage();
    let sql = usage.sql.context("held SQL observer")?;
    ensure!(
        sql.active_cells == 1 && sql.retained_bytes > 0,
        "cleanup released original SQL admission"
    );
    ensure!(
        sql.running_workers == 1,
        "single SQL worker topology changed"
    );
    let extents = usage.serving_images.extents;
    if rooted {
        let extents = extents.context("held physical extent observer")?;
        ensure!(extents.workspaces > 0 && extents.workspace_bytes > 0 && !extents.quarantined);
    }
    Ok(extents)
}

struct Closed {
    node: usize,
    outcome: Result<NodeOutcome>,
    report: std::result::Result<RaftCloseReport, Arc<RaftCloseError>>,
}

async fn close_all(nodes: Vec<Node>, hosts: &[RaftCells]) -> Vec<Closed> {
    let mut rows = Vec::new();
    for (node, (owner, host)) in nodes.into_iter().zip(hosts).enumerate() {
        let outcome = owner.shutdown(ShutdownMode::Preserve).await;
        let report = host.close().await;
        eprintln!(
            "PRIVATE_CLEANUP_LIFECYCLE_CLOSE {}",
            json!({
                "node":node, "nodeOutcome":format!("{outcome:?}"), "hostOutcome":format!("{report:?}"),
            })
        );
        rows.push(Closed {
            node,
            outcome,
            report,
        });
    }
    rows
}

fn validate_close(rows: &[Closed], hosts: &[RaftCells], cut: Cut) -> Result<()> {
    ensure!(
        rows.len() == 3 && rows.iter().enumerate().all(|(node, row)| row.node == node),
        "did not join all three node and host owners"
    );
    for row in rows {
        let outcome = row
            .outcome
            .as_ref()
            .map_err(|error| anyhow::anyhow!("joined node {}: {error:#}", row.node))?;
        ensure!(
            matches!(outcome, NodeOutcome::Stopped(_)),
            "unexpected joined node {} outcome: {outcome:?}",
            row.node
        );
        let sql = hosts[row.node]
            .resource_usage()
            .sql
            .context("joined SQL observer unavailable")?;
        ensure!(
            sql.running_workers == 0
                && sql.active_cells == 0
                && sql.retained_operations == 0
                && sql.queued_or_running == 0,
            "physical SQL owner still live after joined shutdown: {sql:?}"
        );
        match &row.report {
            Ok(report) => {
                ensure!(report.groups_total == 1, "joined group count changed");
                if row.node != 0 {
                    ensure!(
                        report.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers
                            && report.retained_calls_at_shutdown == 0,
                        "healthy peer did not close quiescently"
                    );
                } else if cut == Cut::IoFailure {
                    ensure!(
                        report.mutation_outcomes == MutationOutcomes::Unobserved
                            && report.groups.iter().any(|group| group.failed),
                        "cleanup failure granted clean source proof"
                    );
                } else if report.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers {
                    ensure!(
                        report.retained_calls_at_shutdown == 0
                            && report.groups.iter().all(|group| !group.failed)
                    );
                }
            }
            Err(error) => {
                ensure!(
                    row.node == 0
                        && cut == Cut::IoFailure
                        && error.log.is_none()
                        && error.inventory.is_none(),
                    "unexpected joined host failure: {error:#}"
                );
                let known = match error.controller.as_deref() {
                    Some(CellGroupControllerError::Apply(error)) => {
                        matches!(error.as_ref(), ExecutionError::Storage(_))
                    }
                    Some(CellGroupControllerError::Command(error)) => matches!(
                        error.cause.as_ref(),
                        CellDispatchError::Execution(ExecutionError::Storage(_))
                    ),
                    _ => false,
                };
                ensure!(known, "unrelated controller close failure: {error:#}");
            }
        }
    }
    Ok(())
}

async fn run(
    rooted: bool,
    durability: Durability,
    cut: Cut,
    store: Arc<catalog_fault::Store>,
) -> Result<()> {
    let control = Arc::new(Control::default());
    let injected = Arc::new(AtomicBool::new(false));
    let mut cluster = start(rooted, cut, store, control.clone(), injected.clone()).await?;
    let _release = Release(control.clone());
    *control.prefix.lock().unwrap() = Some(cluster.nodes[0].info().data_dir.clone());
    let domain = cluster.fixtures[0].domain.clone();
    let filesystem = cluster.fixtures[0].filesystem.clone();
    let closed = Mutex::new(Vec::new());
    let observations = Mutex::new(Observed::default());
    let failed_media = Mutex::new(None);
    let work = domain.run(async {
        let group = cluster.history;
        let leader = managed_fixture::leader(&cluster.hosts, group).await?;
        if leader != 0 {
            cluster.hosts[leader].group(group)?.transfer_leader(MemberId::new(1)?).await?;
        }
        cellar::asyncrt::timeout(TIMEOUT, async {
            while managed_fixture::leader(&cluster.hosts, group).await? != 0 {
                cellar::asyncrt::sleep(Duration::from_millis(10)).await;
            }
            Ok::<_, anyhow::Error>(())
        }).await??;
        let source = cluster.hosts[0].cell(CellHistory {
            cluster: group.cluster, namespace: group.namespace,
            cell: CellId::new("Counter:private-cleanup-source")?, incarnation: IncarnationId::new([228; 16])?,
        })?;
        source.enroll(CellEnrollmentRequest { creation: ProposalId::new([228; 16])?, durability }, TIMEOUT).await?;
        let peer = cluster.hosts[1].cell(source.history().clone())?;
        let contract = source.contract("Run")?;
        let options = interleaving::command(231, 1, durability)?;
        ensure!(options.timeout == TIMEOUT, "original command budget changed");
        let ledger = serde_json::to_vec_pretty(&json!({
            "format":1, "cut":cut.name(), "rooted":rooted, "durability":format!("{durability:?}"),
            "history":format!("{:?}", source.history()), "command":format!("{:?}",options.id),
            "contract":format!("{contract:?}"), "input":b"{}".as_slice(), "timeoutMs":options.timeout.as_millis(),
        }))?;
        let ledger_path = cluster.roots[0].path().join("private-cleanup-command-ledger.json");
        let ledger_filesystem = filesystem.clone();
        domain.blocking(move || -> Result<()> {
            ledger_filesystem.write(&ledger_path, &ledger)?;
            ledger_filesystem.sync_all(&ledger_path)?;
            ledger_filesystem.sync_all(ledger_path.parent().context("ledger parent")?)?;
            Ok(())
        })?.await??;
        let completed = AtomicBool::new(false);
        let finished = Notify::new();
        let (result, observed) = tokio::join!(
            async {
                let result = source.command(&contract, b"{}".to_vec(), options.clone()).await;
                if let Err(error) = &result {
                    eprintln!("PRIVATE_CLEANUP_RESPONSE {}", json!({"ambiguous":error.ambiguous,"command":format!("{:?}",error.command),"error":format!("{error:#}")}));
                }
                completed.store(true, Ordering::Release);
                finished.notify_one();
                result
            },
            async {
                let _release = Release(control.clone());
                cellar::asyncrt::timeout(TIMEOUT, control.entered.notified()).await.context("selected real private cleanup")?;
                ensure!(!completed.load(Ordering::Acquire), "original response preceded joined physical cleanup");
                ensure!(!control.cleanup_on_sql_thread.load(Ordering::Acquire), "physical cleanup remained on SQL owner");
                let charge = held(&cluster.hosts[0], rooted)?;
                let before = media(&control, &domain, filesystem.clone()).await?;
                if cut == Cut::IoFailure {
                    *failed_media.lock().unwrap() = Some(before.clone());
                }
                if cut == Cut::WaiterDeadline {
                    cellar::asyncrt::timeout(TIMEOUT, finished.notified()).await.context("original waiter deadline")?;
                    held(&cluster.hosts[0], rooted)?;
                    observations.lock().unwrap().held_after_deadline = true;
                } else {
                    let handle = cluster.hosts[0].group(group)?;
                    let status = handle.replication_status(TIMEOUT).await?;
                    handle.transfer_leadership(LeadershipTransfer {
                        target: MemberId::new(2)?, expected_term: status.term,
                        expected_revision: status.membership_revision,
                    }, TIMEOUT).await?;
                    cellar::asyncrt::timeout(TIMEOUT, async {
                        while managed_fixture::leader(&cluster.hosts, group).await? != 1 {
                            cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                        }
                        Ok::<_, anyhow::Error>(())
                    }).await??;
                    control.release();
                    cellar::asyncrt::timeout(TIMEOUT, finished.notified()).await.context("original cleanup error response")?;
                    ensure!(injected.load(Ordering::Acquire), "real I/O failure hook never returned its error");
                    cellar::asyncrt::timeout(TIMEOUT, async {
                        while cluster.hosts[0].resource_usage().host.controller.quarantined_groups != 1 {
                            cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                        }
                    }).await.context("failed producer quarantine")?;
                    let usage = cluster.hosts[0].resource_usage();
                    ensure!(!usage.host.controller.closing && usage.host.controller.retained_commands >= 1, "failed producer metadata was discarded");
                    observations.lock().unwrap().quarantined = true;
                    if rooted {
                        let retained = usage.serving_images.extents.context("quarantined store observer unavailable")?;
                        let charge = charge.context("original extent charge")?;
                        ensure!(retained.quarantined && retained.workspaces == charge.workspaces && retained.workspace_bytes == charge.workspace_bytes, "failed physical cleanup refunded its workspace");
                        observations.lock().unwrap().full_workspace_charge_retained = true;
                    }
                    let after = media(&control, &domain, filesystem.clone()).await?;
                    ensure!(before == after, "failed cleanup discarded or changed its concrete bytes");
                    observations.lock().unwrap().concrete_bytes_retained = true;
                }
                // Read the original identity; never reissue the failed command.
                let receipt = peer.receipt(CellReceiptRequest {
                    id: options.id, contract: contract.clone(), input: b"{}".to_vec(), durability,
                }, TIMEOUT).await?;
                ensure!(receipt.command() == options.id && receipt.durability() == durability
                    && receipt.receipt().is_some_and(|row| row.result() == b"1"), "original receipt proof/result differs");
                drop(receipt);
                ensure!(read(&peer, 234).await? == 1, "committed value differs");
                observations.lock().unwrap().receipt_verified = true;
                ensure!(control.calls.load(Ordering::Acquire) == 1, "application callback ran twice");
                if cut == Cut::WaiterDeadline {
                    let mut closing = Box::pin(close_all(std::mem::take(&mut cluster.nodes), &cluster.hosts));
                    let pending = cellar::asyncrt::timeout(Duration::from_secs(1), closing.as_mut()).await;
                    let waited = pending.is_err();
                    // Capture checks without returning while a close future owns
                    // nodes. Release and join that SAME future on every branch.
                    let charged = held(&cluster.hosts[0], rooted);
                    control.release();
                    let rows = match pending { Ok(rows) => rows, Err(_) => closing.await };
                    closed.lock().unwrap().extend(rows);
                    observations.lock().unwrap().close_waited_for_cleanup = Some(waited);
                    charged?;
                    ensure!(waited, "node/host shutdown returned while real cleanup was entered");
                }
                Ok::<_, anyhow::Error>(())
            },
        );
        observed?;
        let error = result.err().context("faulted original request acknowledged success")?;
        ensure!(error.command == options.id && error.ambiguous, "lost or failed cleanup invented a definitive noncommit");
        if cut == Cut::WaiterDeadline {
            ensure!(matches!(error.cause.as_ref(), CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)), "original timeout lost its typed deadline cause: {error:#}");
        } else {
            let CellDispatchError::Execution(ExecutionError::Storage(cause)) = error.cause.as_ref() else {
                anyhow::bail!("real cleanup error lost its typed storage cause: {error:#}");
            };
            if rooted {
                ensure!(matches!(cause.downcast_ref::<cellar_ltx::internal::StoreError>(), Some(cellar_ltx::internal::StoreError::Quarantined)));
            } else {
                ensure!(cause.chain().any(|cause| cause.downcast_ref::<io::Error>().is_some_and(|error| error.to_string() == "injected native private cleanup failure")));
            }
        }
        Ok::<_, anyhow::Error>(())
    }).await.map_err(anyhow::Error::from).and_then(std::convert::identity);
    control.release();
    let mut rows = closed.into_inner().unwrap();
    rows.extend(close_all(std::mem::take(&mut cluster.nodes), &cluster.hosts).await);
    // Check actual failed bytes after every node/host has returned, while the
    // test's execution domain can still own and join this final inspection.
    let terminal_bytes = match failed_media.into_inner().unwrap() {
        Some(expected) => match media(&control, &domain, filesystem).await {
            Ok(actual) if actual == expected => {
                observations.lock().unwrap().concrete_bytes_after_join = true;
                Ok(())
            }
            Ok(_) => Err(anyhow::anyhow!(
                "joined shutdown changed failed private bytes"
            )),
            Err(error) => Err(error.context("failed private bytes after joined shutdown")),
        },
        None => Ok(()),
    };
    if let Err(error) = &terminal_bytes {
        eprintln!("private cleanup lifecycle terminal media: {error:#}");
    }
    domain.shutdown().await;
    let joined = validate_close(&rows, &cluster.hosts, cut);
    if let Err(error) = &joined {
        eprintln!("private cleanup lifecycle joined close: {error:#}");
    }
    eprintln!(
        "PRIVATE_CLEANUP_LIFECYCLE {}",
        json!({
            "cut":cut.name(), "rooted":rooted, "durability":format!("{durability:?}"),
            "observations":observations.into_inner().unwrap(), "callbackCalls":control.calls.load(Ordering::Acquire),
            "workSucceeded":work.is_ok(), "joinedPhysicalOwners":joined.is_ok(), "closeRows":rows.len(),
        })
    );
    work?;
    terminal_bytes?;
    joined
}
