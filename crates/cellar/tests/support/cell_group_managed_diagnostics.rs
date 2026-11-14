// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded observations after failure; these never reconcile or retry a mutation.
use super::*;

pub async fn record(cluster: &ManagedCluster, label: &str) {
    record_parts(&cluster.hosts, &cluster.fixtures, cluster.history, label).await;
}

pub(super) async fn record_parts(
    hosts: &[RaftCells],
    fixtures: &[Fixture],
    history: GroupHistory,
    label: &str,
) {
    // Capture all owner counters before awaiting any actor observation. A later
    // status response may arrive after the failed request's owner has unwound.
    let mut samples: Vec<_> = hosts.iter().enumerate().map(|(index, host)| {
        let usage = host.resource_usage();
        let performance = host.performance();
        let counters: std::collections::BTreeMap<_, _> = performance.counters.into_iter()
            .filter(|counter| counter.name.starts_with("cell_"))
            .map(|counter| (counter.name, counter.value)).collect();
        let operations: std::collections::BTreeMap<_, _> = performance.operations.into_iter()
            .filter(|operation| operation.started != 0)
            .map(|operation| (operation.name, serde_json::json!({
                "started": operation.started, "succeeded": operation.succeeded,
                "failed": operation.failed, "abandoned": operation.abandoned,
                "count": operation.count, "sum_us": operation.sum_us,
            }))).collect();
        let store = &fixtures[index].store;
        serde_json::json!({
            "node": hex::encode(host.identity().node.bytes()),
            "controller": usage.host.controller, "runtime": usage.runtime,
            "sql": usage.sql, "control": usage.control, "services": usage.control_services,
            "transport": usage.transport.map(|transport| serde_json::json!({
                "tasks": transport.tasks, "outgoing": transport.outgoing,
                "query_connections": transport.outgoing_connections.query,
            })),
            "performance_available": performance.available, "performance_saturated": performance.saturated,
            "counters": counters, "operations": operations,
            "denied_archive_writes": store.denied_group_archive_writes.load(Ordering::Acquire),
            "denied_routing_reads": store.denied_routing_metadata_reads.load(Ordering::Acquire),
            "active_fleet_reads": store.active_fleet_reads.load(Ordering::Acquire),
            "fleet_cas_waiting": store.fleet_cas_waiting.load(Ordering::Acquire),
        })
    }).collect();
    let observed = futures_util::future::join_all(hosts.iter().map(|host| async move {
        let group = match host.group(history) {
            Ok(group) => group,
            Err(error) => return serde_json::json!({"group_error": format!("{error:?}")}),
        };
        let archive = group
            .archive_status()
            .ok()
            .flatten()
            .and_then(|status| status.published)
            .map(|manifest| format!("{:?}", manifest.position()));
        let status = tokio::time::timeout(
            Duration::from_millis(500),
            group.observe(Duration::from_millis(500)),
        )
        .await;
        serde_json::json!({"status": format!("{status:?}"), "published_archive": archive})
    }))
    .await;
    for (sample, status) in samples.iter_mut().zip(observed) {
        sample["observation"] = status;
    }
    eprintln!(
        "NATIVE_FAILURE_SNAPSHOT {}",
        serde_json::json!({"case": label, "nodes": samples})
    );
}
