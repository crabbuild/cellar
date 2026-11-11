// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A delayed real object-store lookup cannot refill a superseded public route.
use super::*;
use cellar::application::consensus::verification::{cell_groups::CellRoutingFailure, CatalogError};

struct Release(Vec<Arc<fault::RegistryGate>>);

impl Drop for Release {
    fn drop(&mut self) {
        for gate in &self.0 {
            gate.armed.store(false, Ordering::Release);
            gate.resume.notify_one();
        }
    }
}

fn command_rpcs(host: &RaftCells) -> Result<u64> {
    let snapshot = host.performance();
    ensure!(snapshot.available && !snapshot.saturated);
    Ok(snapshot
        .counters
        .into_iter()
        .find(|counter| counter.name == "cell_command_rpc_attempts")
        .context("command RPC counter")?
        .value)
}

pub(super) async fn exercise(
    cluster: &ManagedCluster,
    web: &http::Web,
    callbacks: &AtomicUsize,
    rooted: bool,
    workers: &mut Vec<RaftFleetTransitionService>,
    roots: &mut Vec<tempfile::TempDir>,
) -> Result<()> {
    let Prepared {
        mut cells,
        order,
        coordinator,
        first,
        client,
        retry,
    } = prepare(cluster, rooted, workers, roots).await?;
    let ingress = Ingress {
        cluster,
        web,
        client: &client,
        retry,
    };
    // Keep the soon-to-split interval cold at this ingress. The other interval
    // is already a verified cache entry when publication/generation advances.
    for index in order[3..].iter().copied() {
        ingress
            .write(&mut cells[index], &first[1], index, None)
            .await?;
    }
    let old_index = order[0];
    let new_index = order[1];
    let directory = &cluster.directory;
    let domain = &cluster.fixtures[0].domain;
    let (view, head_reads) = fault::count_reads(domain.run(directory.observe())).await;
    let view = view??;
    let head = head_reads
        .keys()
        .find(|path| path.ends_with("/cell-groups/head"))
        .context("actual directory head GET")?
        .clone();
    let old_route = domain
        .run(directory.lookup(&view, &cells[old_index].history.cell))
        .await??;
    ensure!(old_route.history == first[0].history);
    let store = &cluster.fixtures[0].store;
    store
        .gate_range_tree_change(&object_store::path::Path::from(head.clone()))
        .await?;
    let publication = store.range_tree_change.clone();
    let head_gate = Arc::new(fault::RegistryGate::default());
    let before_callbacks = callbacks.load(Ordering::Acquire);
    let old_options = options(old_index, cells[old_index].sequence, retry)?;
    let original_id = old_options.id;
    let old_history = cells[old_index].history.clone();
    let fresh_history = cells[new_index].history.clone();
    let mut fresh_options = options(new_index, cells[new_index].sequence, retry)?;
    fresh_options.durability = Durability::Quorum;
    let ingress_store = &cluster.fixtures[INGRESS].store;
    let observer = RaftFleetControl::new(
        domain,
        Fixture::open_catalog(domain, store.clone(), cluster.history.cluster)?,
        vec![directory.clone()],
    )?;
    let flow = async {
        let _release = Release(vec![publication.clone(), head_gate.clone()]);
        // Preparation uses the existing transition bound before constructing
        // the old command. Once its physical CAS is held, all client work below
        // fits the original five-second call window; no deadline is renewed.
        tokio::time::timeout(Duration::from_secs(60), publication.entered.notified())
            .await
            .context("physical tree-changing publication CAS entered")?;
        let deadline = tokio::time::Instant::now() + TIMEOUT;
        head_gate.armed.store(true, Ordering::Release);
        *ingress_store.routing_read_after.lock().unwrap() =
            Some((object_store::path::Path::from(head), head_gate.clone()));
        let old = cluster.hosts[INGRESS].cell(old_history)?;
        let old = old.command(
            &old.contract("Increment")?,
            1_i64.to_be_bytes().to_vec(),
            old_options,
        );
        let fresh_work = async {
            let _release = Release(vec![publication.clone(), head_gate.clone()]);
            tokio::time::timeout_at(deadline, head_gate.entered.notified())
                .await
                .context("public command holds the old physical head response")?;
            ensure!(callbacks.load(Ordering::Acquire) == before_callbacks);
            publication.resume.notify_one();
            let operation = hex::encode([138; 16]);
            let namespace = hex::encode(cluster.history.namespace.bytes());
            tokio::time::timeout_at(deadline, async {
                loop {
                    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                    let state = observer
                        .transition_status(&namespace, &operation, remaining)
                        .await?;
                    if matches!(
                        state.phase,
                        Some(TransitionPhase::Active | TransitionPhase::Retired)
                    ) {
                        return Ok::<_, anyhow::Error>(());
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
            .await
            .context("fresh child activation before old call expires")??;
            let fresh = cluster.hosts[INGRESS].cell(fresh_history)?;
            fresh_options.timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
            let receipt = fresh
                .command(
                    &fresh.contract("Increment")?,
                    1_i64.to_be_bytes().to_vec(),
                    fresh_options,
                )
                .await?;
            ensure!(
                receipt.witness.group.group == GroupId::new([140; 16])?
                    && receipt.result == 2_i64.to_be_bytes()
            );
            let before_rpc = command_rpcs(&cluster.hosts[INGRESS])?;
            Ok::<_, anyhow::Error>((before_rpc, receipt))
        };
        let (outcome, fresh) = tokio::join!(old, fresh_work);
        let (before_rpc, receipt) = fresh?;
        let refused = outcome
            .err()
            .context("superseded lookup must refuse before dispatch")?;
        ensure!(refused.command == original_id);
        ensure!(
            matches!(refused.cause.as_ref(), CellDispatchError::Routing(error)
            if matches!(&error.reason, CellRoutingFailure::Catalog(CatalogError::Conflict("superseded routing observation"))))
        );
        let after_rpc = command_rpcs(&cluster.hosts[INGRESS])?;
        ensure!(
            after_rpc == before_rpc && callbacks.load(Ordering::Acquire) == before_callbacks + 1
        );
        Ok::<_, anyhow::Error>(receipt)
    };
    // Both cells selected above share the first new child. The successful fresh
    // lookup therefore covers the held command's cell before its old head response returns.
    let (children, held) = tokio::join!(
        Box::pin(split(
            cluster,
            &coordinator,
            workers,
            first[0].history,
            &cells[order[2]].history,
            [[0, 1, 6], [2, 5, 6]],
            1
        )),
        Box::pin(flow),
    );
    *ingress_store.routing_read_after.lock().unwrap() = None;
    let receipt = held?;
    cells[new_index].latest = receipt;
    cells[new_index].value += 1;
    cells[new_index].sequence += 1;
    let children = children?;
    let current = [children[0].clone(), children[1].clone(), first[1].clone()];
    let before = directory_lookups(cluster)?;
    ingress
        .write(&mut cells[old_index], &children[0], old_index, None)
        .await?;
    ensure!(
        directory_lookups(cluster)? == before,
        "old lookup displaced the newer covering child"
    );
    let untouched = order[5];
    for adapter in [false, true] {
        let before = directory_lookups(cluster)?;
        ingress
            .write(&mut cells[untouched], &first[1], untouched, Some(adapter))
            .await?;
        ensure!(
            directory_lookups(cluster)? == before,
            "old lookup invalidated an unrelated interval"
        );
    }
    verify(cluster, web, &client, &cells, &current, retry, callbacks).await?;
    eprintln!("public old lookup extents={rooted}: physical old-head GET crossed live split publication; fresh child generation refused stale completion without an RPC; original ID replay and unrelated cache retained");
    Ok(())
}
