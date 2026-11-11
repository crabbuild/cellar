// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! An unrelated endpoint update cannot discard an unchanged leader's reply.
use super::*;
use std::sync::atomic::AtomicBool;

struct Release {
    metadata: Arc<fault::Store>,
    archive: Arc<fault::Store>,
    view: Arc<fault::RegistryGate>,
}

impl Release {
    fn archive(&self) {
        self.archive
            .group_archive_puts_hang
            .store(false, Ordering::Release);
        self.archive.group_archive_puts_resume.notify_waiters();
    }
}

impl Drop for Release {
    fn drop(&mut self) {
        *self.metadata.denied_prefix.lock().unwrap() = None;
        *self.metadata.routing_read_after.lock().unwrap() = None;
        self.view.armed.store(false, Ordering::Release);
        self.view.resume.notify_one();
        self.archive();
    }
}

pub(super) async fn exercise(
    cluster: &mut nodes::Cluster,
    callbacks: &AtomicUsize,
    rooted: bool,
) -> Result<()> {
    cluster.authorize_commands()?;
    cluster.authorize_ingress_reads()?;
    cluster.bind()?;
    lead_on_zero(cluster).await?;
    let group = cluster.managed.history;
    let logical = counter::history(group.cluster, group.namespace, "one")?;
    cluster.managed.hosts[0]
        .cell(logical.clone())?
        .enroll(
            CellEnrollmentRequest {
                creation: ProposalId::new([230; 16])?,
                durability: Durability::S3,
            },
            TIMEOUT,
        )
        .await?;
    let cell = cluster.managed.hosts[INGRESS].cell(logical.clone())?;
    let increment = cell.contract("Increment")?;
    let anchor = cell
        .command(&increment, 1_i64.to_be_bytes().to_vec(), options(1)?)
        .await?;
    ensure!(anchor.result == 1_i64.to_be_bytes() && anchor.durability == Durability::S3);
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(TIMEOUT + Duration::from_secs(1))
        .build()?;
    let fixture = &cluster.managed.fixtures[0];
    let catalog = Catalog::new(
        fixture.store.clone(),
        CatalogOptions {
            prefix: "managed/".into(),
            ..Default::default()
        },
    )?;
    let namespace = fixture
        .domain
        .run(catalog.existing_namespace(fixture.declaration.namespace.clone()))
        .await??;
    let suffix = format!("{}/", hex::encode(group.namespace.bytes()));
    let prefix = namespace
        .data_prefix()
        .strip_suffix(suffix.as_str())
        .context("inflight endpoint namespace prefix")?;
    for (ordinal, (caller, adapter)) in
        [("Sdk", None), ("Native", Some(false)), ("Axum", Some(true))]
            .into_iter()
            .enumerate()
    {
        lead_on_zero(cluster).await?;
        let before_view = cluster.managed.fixtures[0]
            .domain
            .run(cluster.managed.directory.observe())
            .await??;
        let before_route = cluster.managed.fixtures[0]
            .domain
            .run(
                cluster
                    .managed
                    .directory
                    .lookup(&before_view, &logical.cell),
            )
            .await??;
        let configuration = cluster.managed.hosts[0]
            .group(group)?
            .quorum_membership(TIMEOUT)
            .await?;
        let revision = configuration.membership().revision();
        let voters = configuration.configuration().clone();
        drop(configuration);
        let leader = cluster.managed.peers[0].clone();
        let old = cluster.managed.peers[1].clone();
        let path = object_store::path::Path::from(format!(
            "{prefix}nodes/{}/view",
            hex::encode(old.node.bytes())
        ));
        let metadata = cluster.managed.fixtures[INGRESS].store.clone();
        let archive = cluster.managed.fixtures[0].store.clone();
        let view = Arc::new(fault::RegistryGate::default());
        let release = Release {
            metadata: metadata.clone(),
            archive: archive.clone(),
            view: view.clone(),
        };
        let denied = metadata.denied_prefix_reads.load(Ordering::Acquire);
        *metadata.denied_prefix.lock().unwrap() = Some(path.clone());
        tokio::time::timeout(TIMEOUT, async {
            while metadata.denied_prefix_reads.load(Ordering::Acquire) == denied {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .context("inflight follower metadata denial entered")?;
        cluster.restart_voter(1, 170 + ordinal as u8).await?;
        lead_on_zero(cluster).await?;
        let current = cluster.managed.peers[1].clone();
        ensure!(
            old.node == current.node
                && old.disk == current.disk
                && old.process != current.process
                && cluster.managed.peers[0] == leader
        );
        let cut = cluster.managed.fixtures[0].catalog.read(TIMEOUT).await?;
        tokio::time::timeout(TIMEOUT, async {
            loop {
                if cluster.managed.hosts[INGRESS]
                    .observe_node(TIMEOUT)
                    .await?
                    .fleet_trust_revision
                    .is_some_and(|r| r >= cut.revision())
                {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .context("inflight ingress installs changed follower boot trust")??;
        let old_inspection = cluster.managed.hosts[INGRESS]
            .inspect_peer_node(old.node, cut.revision(), TIMEOUT)
            .await;
        ensure!(
            matches!(
                old_inspection,
                Err(InspectionError::Refused | InspectionError::StalePeer)
            ),
            "old inflight follower inspection={old_inspection:?}"
        );
        view.armed.store(true, Ordering::Release);
        *metadata.routing_read_after.lock().unwrap() = Some((path, view.clone()));
        *metadata.denied_prefix.lock().unwrap() = None;
        tokio::time::timeout(TIMEOUT, view.entered.notified())
            .await
            .context("new follower NodeView held before admission")?;
        let before = work(cluster)?;
        let before_callbacks = callbacks.load(Ordering::Acquire);
        ensure!(before_callbacks == ordinal + 1);
        let before_archives = archive.group_archive_puts_entered.load(Ordering::Acquire);
        archive
            .group_archive_puts_hang
            .store(true, Ordering::Release);
        let selected = options(ordinal as u64 + 2)?;
        let deadline = tokio::time::Instant::now() + TIMEOUT;
        let done = AtomicBool::new(false);

        enum Reply {
            Sdk(Box<CommandReceipt>),
            Http(serde_json::Value),
        }
        let pending = async {
            let result = async {
                match adapter {
                    Some(axum) => Ok::<_, anyhow::Error>(Reply::Http(
                        http::Web::send(
                            request(cluster, &client, axum, &selected)?,
                            format!("inflight endpoint {caller}"),
                        )
                        .await?
                        .success()?,
                    )),
                    None => Ok(Reply::Sdk(Box::new(
                        cell.command(&increment, 1_i64.to_be_bytes().to_vec(), selected.clone())
                            .await?,
                    ))),
                }
            }
            .await;
            done.store(true, Ordering::Release);
            result
        };
        let change = async {
            tokio::time::timeout_at(deadline, async {
                loop {
                    ensure!(!done.load(Ordering::Acquire), "inflight command finished before controlled endpoint update");
                    if callbacks.load(Ordering::Acquire) == before_callbacks + 1
                        && cluster.managed.hosts[0].resource_usage().host.controller.retained_s3 == 1
                        && archive.group_archive_puts_entered.load(Ordering::Acquire) > before_archives
                    { break; }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                ensure!(work(cluster)?[0] == before[0] + 1);
                // The application command is admitted and awaiting actual S3
                // I/O. Install only the other voter's new authenticated endpoint.
                view.resume.notify_one();
                loop {
                    ensure!(!done.load(Ordering::Acquire), "inflight command finished while its archive proof was held");
                    if cluster.managed.hosts[INGRESS].inspect_peer_node(current.node, cut.revision(), TIMEOUT).await.is_ok() { break; }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                ensure!(cluster.managed.peers[0] == leader && callbacks.load(Ordering::Acquire) == before_callbacks + 1);
                eprintln!("ENDPOINT_INFLIGHT_CUT {}", serde_json::json!({"caller": caller, "extents": rooted, "event": "follower_accepted_while_admitted_s3_held", "command": format!("{:?}", selected.id), "callbacks": callbacks.load(Ordering::Acquire), "retained_s3": cluster.managed.hosts[0].resource_usage().host.controller.retained_s3, "selected_leader_unchanged": true, "membership_revision": format!("{revision:?}"), "directory": hex::encode(before_view.digest())}));
                release.archive();
                Ok::<_, anyhow::Error>(())
            }).await.context("unrelated endpoint update within original command deadline")?
        };
        let (result, changed) = tokio::join!(pending, change);
        let result = result.with_context(|| format!("inflight {caller} after follower endpoint acceptance, cut={changed:?}, command={:?}, before={before:?}, after={:?}, callbacks={}", selected.id, work(cluster), callbacks.load(Ordering::Acquire)))?;
        changed?;
        ensure!(tokio::time::Instant::now() < deadline);
        let after = work(cluster)?;
        let delta: Vec<_> = after.iter().zip(before).map(|(a, b)| a - b).collect();
        ensure!(
            delta[0] == 1 && (1..=4).contains(&delta[1]) && delta[2..] == [0, 0, 0, 0, 0],
            "inflight endpoint routing work={delta:?}"
        );
        let (receipt, http) = match result {
            Reply::Sdk(receipt) => (*receipt, None),
            Reply::Http(reply) => (
                cell.command(&increment, 1_i64.to_be_bytes().to_vec(), selected.clone())
                    .await?,
                Some(reply),
            ),
        };
        ensure!(
            receipt.id == selected.id
                && receipt.result == (ordinal as i64 + 2).to_be_bytes()
                && receipt.durability == Durability::S3
                && receipt.witness.group == group
                && matches!(receipt.version.covers(&anchor.version), Ok(true))
                && callbacks.load(Ordering::Acquire) == before_callbacks + 1
        );
        if let Some(reply) = &http {
            http_receipt(reply, &receipt, ordinal as i64 + 2)?;
        }
        let replay = cell
            .command(&increment, 1_i64.to_be_bytes().to_vec(), selected.clone())
            .await?;
        ensure!(
            replay.id == receipt.id
                && replay.result == receipt.result
                && replay.version == receipt.version
                && replay.witness == receipt.witness
                && replay.durability == receipt.durability
                && callbacks.load(Ordering::Acquire) == before_callbacks + 1
        );
        let read = cell
            .query(
                ProposalId::new([210 + ordinal as u8; 16])?,
                &cell.contract("Get")?,
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::AtLeast(receipt.version_token.clone()),
                    timeout: TIMEOUT,
                    ..Default::default()
                },
            )
            .await?;
        ensure!(read.result == receipt.result && read.witness == receipt.witness);
        let after_view = cluster.managed.fixtures[0]
            .domain
            .run(cluster.managed.directory.observe())
            .await??;
        let after_route = cluster.managed.fixtures[0]
            .domain
            .run(cluster.managed.directory.lookup(&after_view, &logical.cell))
            .await??;
        ensure!(before_view.digest() == after_view.digest() && before_route == after_route);
        let configuration = cluster.managed.hosts[0]
            .group(group)?
            .quorum_membership(TIMEOUT)
            .await?;
        ensure!(
            configuration.membership().revision() == revision
                && configuration.configuration() == &voters
                && cluster.managed.peers[0] == leader
        );
        drop(configuration);
        ensure!(
            cluster.managed.hosts[INGRESS]
                .observe_node(TIMEOUT)
                .await?
                .node
                .groups
                == 0
        );
        eprintln!(
            "ENDPOINT_INFLIGHT_EVIDENCE {}",
            serde_json::json!({"schema": 1, "case": "unrelated_endpoint_keeps_inflight_reply", "event": "verified", "caller": caller, "extents": rooted, "routing_before": before, "routing_after": after, "command": format!("{:?}", receipt.id), "retry_until_ms": selected.retry_until_ms, "required": "S3", "achieved": "S3", "callbacks_before": before_callbacks, "callbacks_after": callbacks.load(Ordering::Acquire), "result_be": hex::encode(&receipt.result), "version": format!("{:?}", receipt.version), "witness": format!("{:?}", receipt.witness), "old_process": hex::encode(old.process.bytes()), "current_process": hex::encode(current.process.bytes()), "leader_unchanged": true, "directory_before": hex::encode(before_view.digest()), "directory_after": hex::encode(after_view.digest()), "membership_revision": format!("{revision:?}")})
        );
    }
    Ok(())
}
