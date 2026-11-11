// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A delayed real object-store lookup cannot refill a superseded public route.
use super::*;
use cellar::application::consensus::verification::{cell_groups::CellRoutingFailure, CatalogError};

use std::sync::atomic::AtomicBool;

async fn delayed_request(
    caller: Caller,
    ingress: &Ingress<'_>,
    history: CellHistory,
    name: &str,
    options: CommandOptions,
) -> Result<()> {
    ensure!(options.durability == Durability::S3 && options.timeout == TIMEOUT);
    match caller {
        Caller::Sdk => {
            let handle = ingress.cluster.hosts[INGRESS].cell(history)?;
            let id = options.id;
            let refused = handle
                .command(
                    &handle.contract("Increment")?,
                    1_i64.to_be_bytes().to_vec(),
                    options,
                )
                .await
                .err()
                .context("superseded lookup must refuse before dispatch")?;
            ensure!(refused.command == id && !refused.ambiguous);
            ensure!(
                matches!(refused.cause.as_ref(), CellDispatchError::Routing(error)
                if matches!(&error.reason, CellRoutingFailure::Catalog(CatalogError::Conflict("superseded routing observation"))))
            );
        }
        Caller::Native | Caller::Axum => {
            http::Web::send(
                ingress.http_request(matches!(caller, Caller::Axum), name, &options),
                format!("delayed old lookup {caller:?} cell={name}"),
            )
            .await?
            .refused(503, "Unavailable")?;
        }
    }
    Ok(())
}

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
    caller: Caller,
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
    // No query-router worker can steal the first physical lookup. The other
    // range is warmed below, after this caller owns its old head response.
    let usage = cluster.hosts[INGRESS].resource_usage();
    ensure!(
        usage.host.controller.groups == 0
            && usage
                .transport
                .context("cold ingress transport")?
                .router_routes
                == 0
    );
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
    let replay_options = old_options.clone();
    let old_name = cells[old_index].name;
    let unchanged_history = cells[order[5]].history.clone();
    let unchanged_receipt = cells[order[5]].original.clone();
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
        let old_done = AtomicBool::new(false);
        let old = async {
            let result =
                delayed_request(caller, &ingress, old_history, old_name, old_options).await;
            old_done.store(true, Ordering::Release);
            result
        };
        let fresh_work = async {
            let _release = Release(vec![publication.clone(), head_gate.clone()]);
            tokio::time::timeout_at(deadline, head_gate.entered.notified())
                .await
                .context("public command holds the old physical head response")?;
            ensure!(
                !old_done.load(Ordering::Acquire)
                    && callbacks.load(Ordering::Acquire) == before_callbacks
            );
            let unchanged = cluster.hosts[INGRESS].cell(unchanged_history)?;
            let read = unchanged
                .query(
                    ProposalId::new([246; 16])?,
                    &unchanged.contract("Get")?,
                    vec![],
                    QueryOptions {
                        consistency: QueryConsistency::AtLeast(
                            unchanged_receipt.version_token.clone(),
                        ),
                        timeout: deadline.saturating_duration_since(tokio::time::Instant::now()),
                        ..Default::default()
                    },
                )
                .await?;
            ensure!(
                read.result == unchanged_receipt.result
                    && read.witness == unchanged_receipt.witness
            );
            tokio::time::timeout_at(deadline, async {
                while cluster.hosts[INGRESS]
                    .resource_usage()
                    .host
                    .controller
                    .ordinary
                    .items
                    > 1
                {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .context("unrelated query owner drained within original call window")?;
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
            ensure!(
                !old_done.load(Ordering::Acquire),
                "old request completed before physical response release"
            );
            let before_rpc = command_rpcs(&cluster.hosts[INGRESS])?;
            Ok::<_, anyhow::Error>((before_rpc, receipt))
        };
        let (outcome, fresh) = tokio::join!(old, fresh_work);
        let (before_rpc, receipt) = fresh?;
        outcome?;
        ensure!(
            tokio::time::Instant::now() < deadline,
            "refusal arrived after the original call window"
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
    let fresh_receipt = held?;
    cells[new_index].latest = fresh_receipt;
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
    if !matches!(caller, Caller::Sdk) {
        let before_callbacks = callbacks.load(Ordering::Acquire);
        let receipt = http::Web::send(
            ingress.http_request(matches!(caller, Caller::Axum), old_name, &replay_options),
            format!("same-ID delayed HTTP reconciliation {caller:?}"),
        )
        .await?
        .success()?;
        http::value(&receipt, old_name, cells[old_index].value)?;
        ensure!(
            receipt["durability"] == "s3"
                && receipt["group"] == hex::encode(children[0].history.group.bytes())
        );
        ensure!(
            callbacks.load(Ordering::Acquire) == before_callbacks
                && directory_lookups(cluster)? == before
        );
    }
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
    let records: Vec<_> = cells
        .iter()
        .enumerate()
        .map(|(index, cell)| {
            serde_json::json!({
                "history": format!("{:?}", cell.history), "value": cell.value,
                "original_required": "S3", "original": receipt(&cell.original),
                "latest_required": if index == new_index { "Quorum" } else { "S3" },
                "latest": receipt(&cell.latest),
            })
        })
        .collect();
    eprintln!(
        "RANGE_ROUTING_EVIDENCE {}",
        serde_json::json!({
            "schema": 1, "case": "old_lookup_cannot_resurrect_parent", "extents": rooted,
            "event": "verified", "caller": format!("{caller:?}"), "cells": records,
            "old_request": {"client": hex::encode(replay_options.id.client.bytes()), "sequence": replay_options.id.sequence(), "required": "S3"},
            "fresh_anchor_required": "Quorum", "stale_completion_without_rpc": true,
            "routes": current.iter().map(|route| format!("{route:?}")).collect::<Vec<_>>(),
            "nodes": cluster.hosts.iter().map(|host| format!("{:?}", host.identity())).collect::<Vec<_>>(),
            "callbacks": callbacks.load(Ordering::Acquire), "directory_final": directory_lookups(cluster)?,
            "ingress_control": cluster.hosts[INGRESS].resource_usage().control,
            "ingress_ordinary": cluster.hosts[INGRESS].resource_usage().host.controller.ordinary,
        })
    );
    eprintln!("public old lookup caller={caller:?}, extents={rooted}: physical old-head GET crossed live split publication; fresh child generation refused stale completion without an RPC; original ID replay and unrelated cache retained");
    Ok(())
}
