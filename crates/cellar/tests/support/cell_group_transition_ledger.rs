// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Reconcile every acknowledged business command after both topology changes.
use super::*;

#[derive(Clone)]
struct Entry {
    operation: String,
    input: Vec<u8>,
    receipt: CommandReceipt,
}

#[derive(Default)]
pub(super) struct Ledger(Mutex<Vec<Entry>>);

impl Ledger {
    pub fn record(&self, operation: &str, input: &[u8], receipt: &CommandReceipt) -> Result<()> {
        ensure!(input.len() <= 64 * 1024, "ledger input bound");
        let mut entries = self.0.lock().unwrap();
        if let Some(original) = entries.iter().find(|entry| {
            entry.receipt.id == receipt.id
                && entry.receipt.version.history == receipt.version.history
        }) {
            ensure!(
                original.operation == operation
                    && original.input == input
                    && original.receipt.result == receipt.result,
                "a retry changed the recorded command, payload or result"
            );
            ensure!(
                original.receipt.durability != Durability::S3
                    || receipt.durability == Durability::S3
            );
        } else {
            ensure!(entries.len() < 64, "ledger entry bound");
            entries.push(Entry {
                operation: operation.into(),
                input: input.into(),
                receipt: receipt.clone(),
            });
        }
        Ok(())
    }
}

pub(super) struct Cached {
    handle: RaftCellHandle,
    previous: GroupHistory,
}

pub(super) async fn authorize(
    cluster: &ManagedCluster,
    destinations: std::ops::Range<usize>,
    origins: std::ops::Range<usize>,
) -> Result<()> {
    let revision = cluster.fixtures[0].catalog.read(TIMEOUT).await?.revision();
    for index in destinations {
        let host = &cluster.hosts[index];
        for peer in &cluster.peers[origins.clone()] {
            let node = peer.node;
            if node == host.identity().node {
                continue;
            }
            crate::managed_fixture::inspect_current(host, node, revision).await?;
            host.grant_query_origin(NamespaceQueryOrigin {
                cluster: cluster.history.cluster,
                namespace: cluster.history.namespace,
                revision: 1,
                peer: peer.clone(),
            })?;
            host.grant_command_origin(NamespaceCommandOrigin {
                cluster: cluster.history.cluster,
                namespace: cluster.history.namespace,
                revision: 1,
                peer: peer.clone(),
            })?;
        }
    }
    Ok(())
}

async fn read(
    handle: &RaftCellHandle,
    id: u8,
    consistency: QueryConsistency,
) -> Result<QueryResult> {
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    let contract = handle.contract("Get")?;
    tokio::time::timeout_at(deadline, async {
        loop {
            let result = handle
                .query(
                    ProposalId::new([id; 16])?,
                    &contract,
                    vec![],
                    QueryOptions {
                        consistency: consistency.clone(),
                        timeout: deadline.saturating_duration_since(tokio::time::Instant::now()),
                        ..Default::default()
                    },
                )
                .await;
            match result {
                Err(error)
                    if matches!(
                        error.as_ref(),
                        CellDispatchError::Runtime(RuntimeError::NotFound | RuntimeError::NotReady)
                    ) =>
                {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                result => return Ok::<_, anyhow::Error>(result?),
            }
        }
    })
    .await
    .context("managed route discovery exceeded the original query deadline")?
}

pub(super) async fn prime(cluster: &ManagedCluster) -> Result<Vec<Cached>> {
    authorize(cluster, 3..6, 0..3).await?;
    let mut cached = Vec::new();
    for index in 0..2 {
        let handle = cluster.hosts[0].cell(cell(index)?)?;
        let query = read(&handle, 166 + index, QueryConsistency::Linearizable).await?;
        ensure!(query.result == [12i64, 13][index as usize].to_be_bytes());
        cached.push(Cached {
            handle,
            previous: query.witness.group,
        });
    }
    ensure!(cached[0].previous != cached[1].previous);
    Ok(cached)
}

async fn values(entries: &[Entry], handles: &[RaftCellHandle], merged: GroupHistory) -> Result<()> {
    for (index, handle) in handles.iter().enumerate() {
        let history = cell(index as u8)?;
        let expected = entries
            .iter()
            .filter(|entry| entry.operation == "Add" && entry.receipt.version.history == history)
            .try_fold(0i64, |value, entry| -> Result<_> {
                value
                    .checked_add(i64::from_be_bytes(entry.input.as_slice().try_into()?))
                    .context("acknowledged counter sum overflow")
            })?;
        let query = read(handle, 164 + index as u8, QueryConsistency::Linearizable).await?;
        ensure!(
            query.result == expected.to_be_bytes() && query.witness.group == merged,
            "lost or duplicated an acknowledged mutation"
        );
    }
    Ok(())
}

pub(super) async fn cached(ledger: &Ledger, cached: &[Cached], merged: GroupHistory) -> Result<()> {
    ensure!(cached.len() == 2 && cached.iter().all(|cache| cache.previous != merged));
    let entries = ledger.0.lock().unwrap().clone();
    let handles = cached
        .iter()
        .map(|cache| cache.handle.clone())
        .collect::<Vec<_>>();
    values(&entries, &handles, merged).await
}

pub(super) async fn closed(cached: &[Cached]) -> Result<()> {
    ensure!(cached.len() == 2);
    for cache in cached {
        let response = cache
            .handle
            .query(
                ProposalId::new([163; 16])?,
                &cache.handle.contract("Get")?,
                vec![],
                QueryOptions::default(),
            )
            .await;
        ensure!(
            matches!(response, Err(error) if matches!(error.as_ref(), CellDispatchError::Runtime(RuntimeError::Closed))),
            "a stopped native handle reopened application admission"
        );
    }
    Ok(())
}

pub(super) async fn audit(
    ledger: &Ledger,
    endpoint: &Endpoint,
    merged: GroupHistory,
) -> Result<()> {
    let entries = ledger.0.lock().unwrap().clone();
    ensure!(entries.len() >= 12, "incomplete movement ledger");
    let mut groups = Vec::new();
    for entry in &entries {
        if !groups.contains(&entry.receipt.witness.group) {
            groups.push(entry.receipt.witness.group);
        }
    }
    ensure!(
        groups.len() >= 4,
        "ledger must span the initial group, both split children and merged group"
    );
    let origin = endpoint
        .hosts
        .iter()
        .find(|host| !host.is_closed())
        .context("live former-parent ingress")?;
    let handles = [origin.cell(cell(0)?)?, origin.cell(cell(1)?)?];
    values(&entries, &handles, merged).await?;
    for (index, entry) in entries.iter().enumerate() {
        let handle = origin.cell(entry.receipt.version.history.clone())?;
        let found = handle
            .receipt(
                CellReceiptRequest {
                    id: entry.receipt.id,
                    contract: handle.contract(&entry.operation)?,
                    input: entry.input.clone(),
                    durability: entry.receipt.durability,
                },
                TIMEOUT,
            )
            .await?;
        let stored = found
            .receipt()
            .context("acknowledged receipt missing after split and merge")?;
        ensure!(
            stored.command() == entry.receipt.id
                && stored.result() == entry.receipt.result.as_slice()
                && found.observation().version.history == entry.receipt.version.history
                && found.observation().version.sequence >= entry.receipt.version.sequence
                && found.observation().witness.group == merged
        );
        let floor = read(
            &handle,
            70 + index as u8,
            QueryConsistency::AtLeast(entry.receipt.version_token.clone()),
        )
        .await?;
        ensure!(
            floor.version.history == entry.receipt.version.history
                && floor.version.sequence >= entry.receipt.version.sequence
                && floor.witness.group == merged
        );
    }
    Ok(())
}
