// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Public SDK observations retain fresh quorum without publishing a receipt.
use super::*;

pub(super) async fn qualify(cluster: &mut ManagedCluster, group: GroupHistory) -> Result<()> {
    let leader = managed_fixture::leader(&cluster.hosts, group).await?;
    let handle = cluster.hosts[leader].group(group)?;
    let before = handle.replication_status(TIMEOUT).await?;
    let committed = handle.status().await?.committed;
    for node in 0..3 {
        public_ok(cluster, node, "Read").await?;
    }
    let after_reads = handle.status().await?.committed;
    let after = handle.replication_status(TIMEOUT).await?;
    // A real public mutation must still traverse the same group and advance it.
    public_ok(cluster, 2, "Write").await?;
    let after_write = handle.status().await?.committed;
    ensure!(
        before.term == after.term && before.membership_revision == after.membership_revision,
        "inline KV read comparison changed leadership or membership"
    );
    ensure!(
        after_write > after_reads,
        "public KV write did not advance the group"
    );
    eprintln!(
        "inline KV read publication: before={committed:?} after_reads={after_reads:?} after_write={after_write:?}"
    );
    ensure!(
        after_reads == committed,
        "inline KV reads appended mutations"
    );

    let stopped = cluster.nodes.split_off(1);
    managed_fixture::close_group_nodes_with_groups(stopped, &cluster.hosts[1..], 2).await?;
    cluster.hosts.truncate(1);
    let (status, _) = public(cluster, 0, "Read").await?;
    ensure!(
        status == 503,
        "inline KV read escaped missing fresh quorum: {status}"
    );
    Ok(())
}

struct ArchiveOutage(Arc<catalog_fault::Store>);

impl Drop for ArchiveOutage {
    fn drop(&mut self) {
        self.0
            .deny_group_archive_writes
            .store(false, Ordering::Release);
    }
}

pub(super) async fn qualify_boundaries(
    cluster: &mut ManagedCluster,
    group: GroupHistory,
) -> Result<()> {
    let leader = managed_fixture::leader(&cluster.hosts, group).await?;
    let owner = cluster.hosts[leader].clone();
    let handle = owner.group(group)?;
    let history = owner
        .resolve_cell_history(
            group.cluster,
            group.namespace,
            CellId::new(cellar::engine_api::kv_cell_scope(NAMESPACE, 0))?,
            QueryOptions {
                timeout: TIMEOUT,
                ..Default::default()
            },
        )
        .await?
        .context("initialized S3 KV history")?;
    let cell = owner.cell(history)?;
    let committed = handle.status().await?.committed;
    for node in 0..3 {
        public_ok(cluster, node, "Read").await?;
    }
    ensure!(
        handle.status().await?.committed == committed,
        "S3 KV reads appended mutations"
    );
    let stale = cell
        .query(
            ProposalId::new([241; 16])?,
            &cell.contract("__cellar_kv_inline_read")?,
            vec![],
            QueryOptions {
                consistency: QueryConsistency::Stale,
                timeout: TIMEOUT,
                ..Default::default()
            },
        )
        .await;
    ensure!(
        stale.is_err_and(|error| error.to_string().contains("KV inline read consistency")),
        "KV inline query did not refuse stale consistency before decoding"
    );
    let sequence = cell.contract("__sqlIngressSequence")?;
    let before = cell
        .query(
            ProposalId::new([242; 16])?,
            &sequence,
            0_u64.to_be_bytes().to_vec(),
            QueryOptions {
                timeout: TIMEOUT,
                ..Default::default()
            },
        )
        .await?;
    let store = cluster.fixtures[0].store.clone();
    let denied = store.denied_group_archive_writes.load(Ordering::Acquire);
    store
        .deny_group_archive_writes
        .store(true, Ordering::Release);
    let outage = ArchiveOutage(store.clone());
    // Keep the ambiguous write's original identity. No mutation is reissued.
    let (status, body) = public(cluster, 0, "Write").await?;
    eprintln!(
        "CELLAR_KV_ARCHIVE_OUTAGE_RESPONSE {}",
        json!({"status":status,"body":body})
    );
    ensure!(
        status == 503,
        "S3 KV write escaped the actual archive outage: {status}"
    );
    let after = cell
        .query(
            ProposalId::new([243; 16])?,
            &sequence,
            0_u64.to_be_bytes().to_vec(),
            QueryOptions {
                timeout: TIMEOUT,
                ..Default::default()
            },
        )
        .await?;
    ensure!(
        after.version.sequence > before.version.sequence
            && handle.status().await?.committed > committed
            && store.denied_group_archive_writes.load(Ordering::Acquire) > denied,
        "archive outage did not cover a newly applied KV mutation"
    );
    let committed = handle.status().await?.committed;
    // The independent metadata query above succeeds at Quorum. Business reads
    // of this S3 namespace still require the selected image's archive proof.
    for node in 0..3 {
        let (status, _) = public(cluster, node, "Read").await?;
        ensure!(
            status == 503,
            "S3 KV read bypassed missing coverage: {status}"
        );
    }
    ensure!(
        handle.status().await?.committed == committed,
        "S3 KV refusals appended mutations"
    );
    drop(outage);
    for node in 0..3 {
        public_ok(cluster, node, "Read").await?;
    }
    named_service::restart(cluster).await?;
    managed_fixture::leader(&cluster.hosts, group).await?;
    for node in 0..3 {
        public_ok(cluster, node, "Read").await?;
    }
    let stopped = cluster.nodes.split_off(1);
    managed_fixture::close_group_nodes_with_groups(stopped, &cluster.hosts[1..], 2).await?;
    cluster.hosts.truncate(1);
    let (status, _) = public(cluster, 0, "Read").await?;
    ensure!(status == 503, "S3 KV read escaped missing quorum: {status}");
    Ok(())
}
